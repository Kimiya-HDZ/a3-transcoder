# worker/transcode.py
from __future__ import annotations
import subprocess, time
from pathlib import Path
from typing import Dict

# ---- Preset → resolution/CRF map ----
PRESET_SPECS: Dict[str, Dict[str, int]] = {
    "mp4-1080p": {"width": 1920, "height": 1080, "crf": 23},
    "mp4-720p":  {"width": 1280, "height": 720,  "crf": 23},
    "mp4-480p":  {"width":  854, "height": 480,  "crf": 24},
    "mp4-360p":  {"width":  640, "height": 360,  "crf": 25},
}

def _args_for_intensity(level: str) -> list[str]:
    """Tune encoder effort/CPU. Keep from A1."""
    level = (level or "high").lower()
    if level == "low":
        return ["-c:v", "libx264", "-preset", "faster",  "-threads", "0"]
    if level == "medium":
        return ["-c:v", "libx264", "-preset", "slow",    "-threads", "0"]
    if level == "max":
        # Heavy — demo only.
        return [
            "-c:v", "libx264", "-preset", "placebo", "-tune", "film", "-threads", "0",
            "-x264-params", "me=tesa:subme=10:merange=64:ref=6:rc-lookahead=60"
        ]
    # default: "high"
    return ["-c:v", "libx264", "-preset", "veryslow", "-threads", "0"]

def run_ffmpeg(in_path: str | Path, out_path: str | Path, *, preset: str = "mp4-720p", intensity: str = "high") -> str:
    """
    Transcode a single output from in_path → out_path using a named preset.
    Returns the string path to the written output file.

    NOTE: This function writes EXACTLY to out_path (idempotent target).
    """
    in_path = Path(in_path)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True) # safety

    spec = PRESET_SPECS.get(preset, PRESET_SPECS["mp4-720p"])
    width, height, crf = spec["width"], spec["height"], spec["crf"]

    # If you want audio, remove "-an" and add audio codec (see below).
    video_args = [
        "-vf", f"scale={width}:{height}:flags=lanczos",
        *_args_for_intensity(intensity),
        "-crf", str(crf),
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        "-an",  # drop audio; delete this and add "-c:a", "aac", "-b:a", "128k" to keep audio
    ]

    cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", str(in_path), *video_args, str(out_path)]
    t0 = time.time()
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed ({proc.returncode}). Cmd: {' '.join(cmd)}\n{proc.stderr.strip()}")

    dt = round(time.time() - t0, 2)

    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "ffmpeg failed")

    # Optionally log or return timing:
    # print(f"Transcoded {in_path.name} → {out_path.name} in {dt}s")
    return str(out_path)