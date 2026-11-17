import asyncio
from urllib.parse import urlsplit

from whisperlivekit.ffmpeg_manager import FFmpegManager


class RTSPInputError(Exception):
    """A source error safe to send to clients."""


def parse_rtsp_url(message):
    url = message.get("url") if isinstance(message, dict) else None
    if isinstance(url, str) and len(url) <= 4096 and not any(
        character.isspace() or ord(character) < 32 for character in url
    ):
        try:
            parsed = urlsplit(url)
            if (url.startswith("rtsp://") and parsed.hostname and not parsed.fragment
                    and (parsed.port is None or parsed.port > 0)):
                return url
        except ValueError:
            pass
    raise RTSPInputError("Enter a valid rtsp:// stream URL.")


async def feed_rtsp_audio(url, processor, on_audio):
    ffmpeg = FFmpegManager(sample_rate=processor.sample_rate, channels=processor.channels)
    received_audio = False
    try:
        if not await ffmpeg.start(rtsp_url=url):
            raise RTSPInputError("Could not start RTSP audio. Check that FFmpeg is installed on the server.")
        process = ffmpeg.process
        # Read 100 ms of signed 16-bit PCM at a time, keeping chunks sample-aligned.
        chunk_bytes = processor.sample_rate * processor.channels * 2 // 10
        while True:
            try:
                chunk = await asyncio.wait_for(process.stdout.readexactly(chunk_bytes), timeout=20)
            except asyncio.IncompleteReadError as exc:
                chunk = exc.partial[:len(exc.partial) // 2 * 2]
                if chunk:
                    await processor.process_audio(chunk)
                    received_audio = True
                break
            except asyncio.TimeoutError:
                raise RTSPInputError("Timed out waiting for RTSP audio.") from None
            if not received_audio:
                await on_audio()
                received_audio = True
            await processor.process_audio(chunk)
        if await process.wait() != 0 or not received_audio:
            raise RTSPInputError("Could not read RTSP audio. Check the stream URL, credentials, and audio track.")
    finally:
        if ffmpeg.process is not None and ffmpeg.process.returncode is None:
            try:
                ffmpeg.process.terminate()
            except ProcessLookupError:
                pass
        await ffmpeg.stop()
