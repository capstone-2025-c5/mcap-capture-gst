import time
import foxglove
from foxglove.channels import CompressedVideoChannel
from foxglove.schemas import CompressedVideo, Timestamp
import json
import gi
gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib
import argparse


parser = argparse.ArgumentParser(description="Capture webcam video to MCAP file using Foxglove SDK and GStreamer")
parser.add_argument("-c","--camera-index", type=int, default=0, help="first camera index to use (default: 0)")
parser.add_argument("-m", "--mac", action="store_true", help="run the macos pipeline")
parser.add_argument("--dual", action="store_true", help="start two pipelines: camera-index and camera-index+1")
args = parser.parse_args()

Gst.init(None)

pipeline_str_mac = f"""
avfvideosrc device-index={{idx}} !
video/x-raw,format=UYVY,width=1280,height=720,framerate=15/1 !
videoconvert !
x264enc tune=zerolatency bitrate=4000 speed-preset=veryfast !
h264parse config-interval=1 !
video/x-h264,stream-format=byte-stream,alignment=au !
appsink name=sink emit-signals=true max-buffers=1 drop=true
"""

pipeline_str = f"""
nvarguscamerasrc sensor_id={{idx}} !
video/x-raw(memory:NVMM),width=1920,height=1080,framerate=30/1,format=NV12 !
nvvidconv flip-method=0 ! video/x-raw,width=960,height=720 !
nvv4l2h264enc bitrate=8000000 !
h264parse config-interval=1 !
video/x-h264,stream-format=byte-stream,alignment=au !
appsink name=sink emit-signals=true max-buffers=1 drop=true
"""

import threading


def make_pipeline(idx: int):
    """Create and return (pipeline, appsink) for camera index `idx`."""
    try:
        raw = pipeline_str_mac if args.mac else pipeline_str
        p = Gst.parse_launch(raw.format(idx=idx))
        sink = p.get_by_name("sink")
        p.set_state(Gst.State.PLAYING)
        return p, sink
    except Exception as e:
        print(f"⚠️ Failed to create pipeline for index {idx}: {e}")
        return None, None


# If --dual is provided, run two pipelines (camera_index and camera_index+1). Otherwise run one.
indices = [args.camera_index, args.camera_index + 1] if args.dual else [args.camera_index]

pipelines = []
appsinks = []
for idx in indices:
    p, s = make_pipeline(idx)
    if p is None:
        # if pipeline creation failed, stop starting more
        continue
    pipelines.append((idx, p))
    appsinks.append((idx, s))

mcap_file = f"camera_{args.camera_index}_{time.strftime('%Y%m%d_%H%M%S')}.mcap"

print(f"🎥 Recording webcam to {mcap_file}... Press Ctrl+C to stop")

try:
    # Create an explicit context and open the MCAP writer bound to it. This allows
    # channels created with the same context to be recorded to the file.
    ctx = foxglove.Context()

    server = foxglove.start_server(context=ctx)

    # Open the MCAP writer once and keep it open while recording
    with foxglove.open_mcap(mcap_file, context=ctx) as writer:
        # Create one CompressedVideo channel per camera so they are separate in the MCAP
        channels = {}
        for idx, _ in appsinks:
            topic = f"/camera/{idx}/image/compressed"
            channels[idx] = CompressedVideoChannel(topic=topic, context=ctx)

        stop_event = threading.Event()
        threads = []

        def capture_loop(idx, sink, channel):
            """Pull samples from a single appsink and log them to the provided channel."""
            frame_seq_local = 0
            while not stop_event.is_set():
                try:
                    sample = sink.emit("pull-sample")
                except Exception:
                    # appsink might raise when pipeline stops; break to exit cleanly
                    break

                if sample is None:
                    # small sleep to avoid busy-loop when no frames
                    time.sleep(0.001)
                    continue

                buf = sample.get_buffer()
                if buf is None:
                    continue

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
                        frame_seq_local += 1
                        frame_id = f"seq:{frame_seq_local}"

                timestamp_ns = Timestamp.from_epoch_secs(time.time())

                img_msg = CompressedVideo(
                    timestamp=timestamp_ns,
                    data=data,
                    format="h264",
                    frame_id=frame_id,
                )

                try:
                    channel.log(img_msg)
                except Exception as e:
                    # If logging fails (writer closed etc), stop
                    print(f"⚠️ Failed to log frame for camera {idx}: {e}")
                    break

        # Start one thread per active appsink
        for idx, sink in appsinks:
            ch = channels.get(idx)
            if ch is None or sink is None:
                continue
            t = threading.Thread(target=capture_loop, args=(idx, sink, ch), daemon=True)
            threads.append(t)
            t.start()

        print(f"Started {len(threads)} capture thread(s)")

        # Wait until interrupted
        try:
            while any(t.is_alive() for t in threads):
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping recording...")
            stop_event.set()
            # give threads time to finish
            for t in threads:
                t.join(timeout=1.0)

except Exception as e:
    print(f"Unexpected error: {e}")
    raise

finally:
    # Tear down pipelines
    for idx, p in pipelines:
        try:
            p.set_state(Gst.State.NULL)
        except Exception:
            pass
    print("✅ Recording saved to", mcap_file)