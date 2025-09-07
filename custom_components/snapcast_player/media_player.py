"""Snapcast media player with real-time volume control.

This module provides a Home Assistant media player integration for Snapcast
that supports real-time volume control using a two-process pipeline approach.
"""

from __future__ import annotations
import asyncio
import os.path
import re
import signal
from asyncio import IncompleteReadError
from asyncio.subprocess import PIPE
from dataclasses import dataclass
from datetime import time, timedelta as delta, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

import m3u8
import voluptuous as vol

from homeassistant.components.media_player.const import RepeatMode
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
import homeassistant.helpers.config_validation as cv
from homeassistant.components import media_source
from homeassistant.components.media_player import (
    ATTR_MEDIA_ANNOUNCE,
    PLATFORM_SCHEMA,
    MediaPlayerEntity,
    MediaPlayerEntityFeature,
    MediaPlayerState,
    MediaType,
    async_process_play_media_url,
)
from homeassistant.helpers import aiohttp_client
from homeassistant.helpers.reload import setup_reload_service
from homeassistant.util.dt import utcnow

from .const import (
    CONF_START_DELAY,
    CONF_HOST,
    CONF_NAME,
    CONF_PORT,
    DEFAULT_PORT,
    DOMAIN,
)

if TYPE_CHECKING:
    from asyncio.subprocess import Process

    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
    from homeassistant.helpers.entity_platform import AddEntitiesCallback


PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_HOST): cv.string,
        vol.Optional(CONF_START_DELAY): cv.string,
        vol.Optional(CONF_PORT): cv.string,
        vol.Optional(CONF_NAME): cv.string,
    }
)
METADATA_REGEXES = (
    re.compile(r"^(TITLE)=(.+)$", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^(ARTIST)=(.+)$", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^(ALBUM)=(.+)$", re.MULTILINE | re.IGNORECASE),
)
TITLE_REGEXES = (
    re.compile(r"^StreamTitle=(.+)$", re.MULTILINE),
    re.compile(r"^icy-name=(.+)$", re.MULTILINE),
)
DURATION_REGEX = re.compile(r"^ {2}Duration: ([\d:.]+),", re.MULTILINE)
PROGRESS_REGEX = re.compile(r"time=(\d{2}:\d{2}:\d{2}\.\d{2})")
BUF_SIZE = 64 * 1024 * 1024


@dataclass
class MediaInfo:
    title: str
    artist: str | None = None
    album: str | None = None


@dataclass
class PlaylistInfo:
    items: list[str]
    album_art: str | None


def to_seconds(value: str) -> int:
    t = time.fromisoformat(f"{value}0000")
    return round(
        delta(
            hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond
        ).total_seconds()
    )


def setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    setup_reload_service(hass, DOMAIN, ["media_player"])
    name = config.get(CONF_NAME, DOMAIN)
    host = config.get(CONF_HOST)
    port = config.get(CONF_PORT, DEFAULT_PORT)
    start_delay = config.get(CONF_START_DELAY)

    player_entity = SnapcastPlayer(host, name, port, start_delay, hass)

    def _shutdown(call):
        player_entity.media_stop()

    hass.bus.listen_once(EVENT_HOMEASSISTANT_STOP, _shutdown)
    add_entities([player_entity])


async def parse_playlist(hass: HomeAssistant, url: str) -> PlaylistInfo:
    dirname = os.path.dirname(url)
    if url.startswith("/media/local"):
        url = url.replace("/local", "", 1)
        with open(url) as playlist_file:
            playlist_data = playlist_file.read(64 * 1024)
    else:
        session = aiohttp_client.async_get_clientsession(hass, verify_ssl=False)
        async with session.get(url, timeout=5) as resp:
            charset = resp.charset or "utf-8"
            playlist_data = (await resp.content.read(64 * 1024)).decode(charset)

    custom_tags = {}

    def tag_handler(line: str, *_) -> bool:
        for tag in ("EXTIMG", "EXTVLCOPT"):
            if line.startswith(f"#{tag}:"):
                custom_tags[tag] = line.split(":")[1]
                return True
        return False

    playlist = m3u8.loads(playlist_data, custom_tags_parser=tag_handler)
    album_art = custom_tags.get("EXTIMG")
    return PlaylistInfo(
        [os.path.join(dirname, item.uri) for item in playlist.segments],
        os.path.join(dirname, album_art) if album_art is not None else None,
    )


class SnapcastPlayer(MediaPlayerEntity):
    _attr_media_content_type = MediaType.MUSIC

    def __init__(
        self,
        host: str,
        name: str | None,
        port: str | None,
        start_delay: str | None,
        hass: HomeAssistant,
    ) -> None:
        self._host = host
        self._port = port or DEFAULT_PORT
        self._attr_state = MediaPlayerState.IDLE
        self._name = name
        self._start_delay = start_delay
        self._uri: str | None = None
        self._proc: Process | None = None
        self._is_stopped = False
        self._media_info: MediaInfo | None = None
        self._attr_unique_id = name
        self.hass = hass
        self._queue: list[str] = []
        self._seek_position: float | None = None
        self._attr_volume_level: float = 1.0
        self._attr_is_volume_muted: bool = False
        self._volume_control_enabled: bool = False
        self._volume_proc: Process | None = None

    async def async_play_media(
        self, media_type: MediaType | str, media_id: str, **kwargs: Any
    ) -> None:
        # TODO: Support queuing items
        if media_source.is_media_source_id(media_id):
            sourced_media = await media_source.async_resolve_media(
                self.hass, media_id, self.entity_id
            )
            media_id = sourced_media.url

        if (
            kwargs.get(ATTR_MEDIA_ANNOUNCE)
            and self._attr_state == MediaPlayerState.PLAYING
        ):
            is_live_content = self.media_duration is None
            self._proc = await self._start_playback(media_id, announcement=True)
            self.hass.async_create_task(self._on_announcement_complete(is_live_content))
            return

        self._queue = []
        self._attr_media_image_url = None
        if media_id.endswith(".m3u8") or media_id.endswith(".m3u"):
            playlist = await parse_playlist(self.hass, media_id)
            if not playlist.items:
                return
            for item in playlist.items:
                self._queue.append(item)
            self._uri = self._queue[0]
            if playlist.album_art:
                self._attr_media_image_url = async_process_play_media_url(
                    self.hass, playlist.album_art
                )
        else:
            self._uri = media_id

        if self._uri:
            self._proc = await self._start_playback(self._uri)
            self.hass.async_create_task(self._on_process_complete())

    @property
    def _next_track(self) -> str | None:
        if self._uri:
            try:
                cur_index = self._queue.index(self._uri)
                return self._queue[cur_index + 1]
            except (ValueError, IndexError):
                pass
        return None

    async def _on_announcement_complete(self, is_live_content: bool):
        if self._proc is not None and self._proc.returncode is None:
            await self._proc.wait()
        if self._uri:
            self._proc = await self._start_playback(
                self._uri,
                position=None if is_live_content else self._attr_media_position,
            )
            self.hass.async_create_task(self._on_process_complete())

    async def _on_process_complete(self):
        while True:
            returncode = await self._proc.wait()
            repeat_modes = [RepeatMode.ONE, RepeatMode.ALL]
            if returncode != 0 or (
                self._attr_repeat not in repeat_modes and self._next_track is None
            ):
                self.hass.async_create_task(self.async_update())
                return
            if self._next_track:
                self._uri = self._next_track
            if self._uri:
                self._proc = await self._start_playback(self._uri)

    @property
    def _previous_track(self) -> str | None:
        if self._uri:
            try:
                cur_index = self._queue.index(self._uri)
                if cur_index > 0:
                    return self._queue[cur_index - 1]
                return self._queue[cur_index]
            except ValueError:
                pass
        return None

    async def async_media_seek(self, position: float) -> None:
        if self._uri:
            self._proc = await self._start_playback(self._uri, position)
            self.hass.async_create_task(self._on_process_complete())

    async def async_media_next_track(self) -> None:
        if self._next_track:
            self._uri = self._next_track
        if self._uri:
            self._proc = await self._start_playback(self._uri)
            self.hass.async_create_task(self._on_process_complete())

    async def async_media_previous_track(self) -> None:
        if self._previous_track:
            self._uri = self._previous_track
        if self._uri:
            self._proc = await self._start_playback(self._uri)
            self.hass.async_create_task(self._on_process_complete())

    async def _get_metadata(self) -> MediaInfo | None:
        if self._uri is None:
            return None
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-i",
            async_process_play_media_url(self.hass, self._uri),
            "-f",
            "ffmetadata",
            "-",
            stderr=PIPE,
            stdout=PIPE,
            limit=BUF_SIZE,
            close_fds=True,
        )
        stdout, stderr = await proc.communicate()
        # Parse the ffmpeg output
        if proc.returncode == 0:
            stream_info = stderr.decode("utf-8", errors="ignore")
            duration = None
            if match := DURATION_REGEX.search(stream_info):
                duration = to_seconds(match.group(1))
            self._attr_media_duration = duration
            metadata = stdout.decode("utf-8", errors="ignore")
            details = {}
            for regex in METADATA_REGEXES:
                if match := regex.search(metadata):
                    details[match.group(1).lower()] = match.group(2)
            if details:
                try:
                    return MediaInfo(**details)
                except TypeError:
                    pass
            for title_regex in TITLE_REGEXES:
                if match := title_regex.search(metadata):
                    return MediaInfo(match.group(1))
            if self._uri.startswith("/media/local"):
                return MediaInfo(
                    unquote(os.path.splitext(os.path.basename(self._uri))[0])
                )
        return None

    async def _read_ffmpeg_progress(self):
        while True:
            if (
                self._proc
                and self._proc.returncode is None
                and not self._proc.stderr.at_eof()
            ):
                try:
                    data = await self._proc.stderr.readuntil(b"\r")
                except IncompleteReadError:
                    return
                if match := PROGRESS_REGEX.search(
                    data.decode("utf-8", errors="ignore")
                ):
                    position = to_seconds(match.group(1))
                    if self._seek_position:
                        position += round(self._seek_position)
                    self._attr_media_position = position
                    self._attr_media_position_updated_at = utcnow()
            else:
                self._attr_media_position = None
                return

    async def _start_playback(
        self, uri: str, position: float | None = None, announcement: bool = False
    ) -> Process:
        if self._proc and self._proc.returncode is None:
            self._proc.terminate()
            self._is_stopped = False
        if (
            hasattr(self, "_volume_proc")
            and self._volume_proc
            and self._volume_proc.returncode is None
        ):
            self._volume_proc.terminate()

        # Create a pipeline: ffmpeg -> sox (for volume control) -> output
        # This allows real-time volume adjustment without restarting the main process

        format_args = [
            "-f",
            "s16le",  # Raw PCM format for piping to sox
            "-acodec",
            "pcm_s16le",
            "-ac",
            "2",
            "-ar",
            "48000",
        ]

        # Build audio filter chain (without volume - sox will handle that)
        audio_filters = []

        # Add delay filter if configured
        if self._start_delay is not None:
            audio_filters.append(f"adelay={self._start_delay}:all=true")

        # Combine filters
        filter_args = ["-af", ",".join(audio_filters)] if audio_filters else []

        seek_args = ["-ss", str(timedelta(seconds=round(position)))] if position else []
        self._seek_position = position

        # First process: ffmpeg decoding to stdout
        ffmpeg_args = [
            "ffmpeg",
            "-y",
            "-nostdin",
            *seek_args,
            "-i",
            async_process_play_media_url(self.hass, uri),
            *format_args,
            *filter_args,
            "-",  # Output to stdout
        ]

        # Second process: sox for real-time volume control
        current_volume = 0 if self._attr_is_volume_muted else self._attr_volume_level
        if self._host.startswith("/"):
            out_arg = self._host
        else:
            out_arg = f"tcp://{self._host}:{self._port}"
            # Debug logging
            import logging

            _LOGGER = logging.getLogger(__name__)
            _LOGGER.debug(
                f"Snapcast player connecting to {out_arg} (host={self._host}, port={self._port})"
            )

        sox_args = [
            "sox",
            "-t",
            "raw",
            "-r",
            "48000",
            "-c",
            "2",
            "-e",
            "signed-integer",
            "-b",
            "16",
            "-",  # Input from stdin
            "-t",
            "raw",
            "-r",
            "48000",
            "-c",
            "2",
            "-e",
            "signed-integer",
            "-b",
            "16",
            out_arg,
            "vol",
            str(current_volume),
        ]

        # Start ffmpeg process
        ffmpeg_proc = await asyncio.create_subprocess_exec(
            *ffmpeg_args, stdout=PIPE, stderr=PIPE, limit=BUF_SIZE, close_fds=True
        )

        # Start sox process with ffmpeg output as input
        sox_proc = await asyncio.create_subprocess_exec(
            *sox_args,
            stdin=ffmpeg_proc.stdout,
            stderr=PIPE,
            limit=BUF_SIZE,
            close_fds=True,
        )

        # Close ffmpeg stdout in parent to avoid deadlock
        if ffmpeg_proc.stdout:
            ffmpeg_proc.stdout.close()

        # Store both processes
        self._proc = ffmpeg_proc
        self._volume_proc = sox_proc
        self._volume_control_enabled = True
        self._attr_state = MediaPlayerState.PLAYING

        if not announcement:
            self._attr_media_position = (
                round(self._seek_position) if self._seek_position else 0
            )
            self._attr_media_position_updated_at = utcnow()
            self.hass.async_create_task(self._read_ffmpeg_progress())

        return ffmpeg_proc

    @property
    def media_artist(self) -> str | None:
        return self._media_info.artist if self._media_info else None

    @property
    def media_album_name(self) -> str | None:
        return self._media_info.album if self._media_info else None

    def set_repeat(self, repeat: RepeatMode) -> None:
        self._attr_repeat = repeat

    async def async_update(self):
        if self._proc is not None and self._proc.returncode is None:
            self._attr_state = (
                MediaPlayerState.PAUSED
                if self._is_stopped
                else MediaPlayerState.PLAYING
            )
            self._media_info = await self._get_metadata()
        else:
            self._is_stopped = False
            self._media_info = None
            self._attr_media_position = None
            self._attr_media_duration = None
            self._attr_repeat = RepeatMode.OFF
            self._attr_state = MediaPlayerState.IDLE
            self._attr_media_image_url = None
            # Note: Volume level and mute state are preserved across playback sessions

    @property
    def media_content_id(self) -> str | None:
        return self._uri

    @property
    def name(self):
        return self._name

    @property
    def media_title(self) -> str | None:
        return self._media_info.title if self._media_info else None

    def media_stop(self) -> None:
        if self._proc is not None:
            self._queue = []
            self._proc.terminate()
        if hasattr(self, "_volume_proc") and self._volume_proc is not None:
            self._volume_proc.terminate()
        self._volume_control_enabled = False
        self.hass.create_task(self.async_update())

    def media_pause(self) -> None:
        if self._proc is not None and self._proc.returncode is None:
            self._proc.send_signal(signal.SIGSTOP)
            self._is_stopped = True
            self._attr_state = MediaPlayerState.PAUSED

    def media_play(self) -> None:
        if self._proc is not None and self._proc.returncode is None:
            self._proc.send_signal(signal.SIGCONT)
            self._is_stopped = False
            self._attr_state = MediaPlayerState.PLAYING

    async def _restart_volume_process(self) -> None:
        """Restart just the volume control process with new volume settings."""
        if (
            not self._volume_control_enabled
            or not self._proc
            or self._proc.returncode is not None
        ):
            return

        # Terminate existing sox process
        if self._volume_proc and self._volume_proc.returncode is None:
            self._volume_proc.terminate()

        # Calculate current volume
        current_volume = 0 if self._attr_is_volume_muted else self._attr_volume_level

        # Determine output destination
        if self._host.startswith("/"):
            out_arg = self._host
        else:
            out_arg = f"tcp://{self._host}:{self._port}"
            # Debug logging
            import logging

            _LOGGER = logging.getLogger(__name__)
            _LOGGER.debug(
                f"Snapcast volume process connecting to {out_arg} (host={self._host}, port={self._port})"
            )

        # Start new sox process with updated volume
        sox_args = [
            "sox",
            "-t",
            "raw",
            "-r",
            "48000",
            "-c",
            "2",
            "-e",
            "signed-integer",
            "-b",
            "16",
            "-",  # Input from stdin (ffmpeg stdout)
            "-t",
            "raw",
            "-r",
            "48000",
            "-c",
            "2",
            "-e",
            "signed-integer",
            "-b",
            "16",
            out_arg,
            "vol",
            str(current_volume),
        ]

        try:
            # Create new sox process connected to existing ffmpeg
            self._volume_proc = await asyncio.create_subprocess_exec(
                *sox_args,
                stdin=self._proc.stdout,
                stderr=PIPE,
                limit=BUF_SIZE,
                close_fds=True,
            )
        except Exception:
            # If sox restart fails, fall back to restarting entire pipeline
            if self._uri:
                current_position = self._attr_media_position
                self._proc = await self._start_playback(self._uri, current_position)
                self.hass.async_create_task(self._on_process_complete())

    async def async_set_volume_level(self, volume: float) -> None:
        """Set volume level, range 0..1."""
        self._attr_volume_level = volume
        self._attr_is_volume_muted = False

        # Restart volume process with new volume
        if self._volume_control_enabled:
            await self._restart_volume_process()

    async def async_mute_volume(self, mute: bool) -> None:
        """Mute (true) or unmute (false) media player."""
        self._attr_is_volume_muted = mute

        # Restart volume process with new mute state
        if self._volume_control_enabled:
            await self._restart_volume_process()

    @property
    def supported_features(self) -> MediaPlayerEntityFeature:
        features = (
            MediaPlayerEntityFeature.PLAY_MEDIA
            | MediaPlayerEntityFeature.BROWSE_MEDIA
            | MediaPlayerEntityFeature.STOP
            | MediaPlayerEntityFeature.REPEAT_SET
            | MediaPlayerEntityFeature.VOLUME_SET
            | MediaPlayerEntityFeature.VOLUME_MUTE
        )
        if self._proc and self._proc.returncode is None and self.media_duration:
            features |= MediaPlayerEntityFeature.PLAY
            features |= MediaPlayerEntityFeature.PAUSE
            features |= MediaPlayerEntityFeature.SEEK
        if self._next_track:
            features |= MediaPlayerEntityFeature.NEXT_TRACK
        if self._previous_track:
            features |= MediaPlayerEntityFeature.PREVIOUS_TRACK
        return features

    async def async_browse_media(
        self, media_content_type: str | None = None, media_content_id: str | None = None
    ):
        return await media_source.async_browse_media(
            self.hass,
            media_content_id,
            content_filter=lambda item: item.media_content_type.startswith("audio/"),
        )
