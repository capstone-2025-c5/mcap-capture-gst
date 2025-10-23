import time
import foxglove
from foxglove.channels import CompressedVideoChannel
from foxglove.schemas import CompressedVideo, Timestamp
import json
import gi
gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib

Gst.init(None)

pipeline_str = """
avfvideosrc device-index=0 !
video/x-raw,format=UYVY,width=1280,height=720,framerate=15/1 !
videoconvert !
x264enc tune=zerolatency bitrate=4000 speed-preset=veryfast !
video/x-h264,stream-format=byte-stream !
appsink name=sink emit-signals=true max-buffers=1 drop=true
"""

pipeline = Gst.parse_launch(pipeline_str)
appsink = pipeline.get_by_name("sink")

pipeline.set_state(Gst.State.PLAYING)

print("🎥 Recording webcam to webcam.mcap... Press Ctrl+C to stop")

try:
    # Create an explicit context and open the MCAP writer bound to it. This allows
    # channels created with the same context to be recorded to the file.
    ctx = foxglove.Context()

    server = foxglove.start_server(context=ctx)

    # Open the MCAP writer once and keep it open while recording
    # allow_overwrite=True so repeated runs replace the file instead of erroring
    with foxglove.open_mcap("webcam.mcap", allow_overwrite=True, context=ctx) as writer:
        # Create the image channel bound to the same context so messages are recorded
        image_channel = CompressedVideoChannel(topic="/camera/image/compressed", context=ctx)

        frame_seq = 0  # approach A: simple per-run frame counter

        frame_seq = 0  # fallback counter

        while True:
            sample = appsink.emit("pull-sample")
            if sample is None:
                continue

            buf = sample.get_buffer()
            data = buf.extract_dup(0, buf.get_size())

            # prefer PTS (presentation timestamp) as a stable frame identifier
            pts = buf.pts
            if pts != Gst.CLOCK_TIME_NONE:
                frame_id = f"pts:{int(pts)}"
            else:
                dts = buf.dts
                if dts != Gst.CLOCK_TIME_NONE:
                    frame_id = f"dts:{int(dts)}"
                else:
                    frame_seq += 1
                    frame_id = f"seq:{frame_seq}"

            timestamp_ns = Timestamp.from_epoch_secs(time.time())

            # Wrap as Foxglove CompressedVideo (H.264 annex-B byte stream)
            img_msg = CompressedVideo(
                timestamp=timestamp_ns,
                data=data,
                format="h264",
                frame_id=frame_id,
            )

            image_channel.log(img_msg)

except KeyboardInterrupt:
    print("\n🛑 Stopping recording...")

finally:
    pipeline.set_state(Gst.State.NULL)
    print("✅ Recording saved to webcam.mcap")