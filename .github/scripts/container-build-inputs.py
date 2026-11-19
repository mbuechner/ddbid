import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


BUILD_INPUTS_LABEL = "de.ddb.labs.ddbid.build-inputs"
TAGGED_IMAGE = re.compile(r"(?:[a-z0-9.-]+(?::[0-9]+)?/)?[a-z0-9][a-z0-9./_-]*:[A-Za-z0-9_.-]+")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def inspect(image, *options, allow_missing=False):
    result = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", *options, image],
        capture_output=True,
    )
    if result.returncode:
        error = result.stderr.decode(errors="replace").strip()
        if allow_missing and (
            f"{image.lower()}: not found" in error.lower()
            or "manifest unknown" in error.lower()
        ):
            return None
        raise SystemExit(error or f"Could not inspect {image}.")
    return json.loads(result.stdout)


def base_images(dockerfile):
    images = []
    for line in dockerfile.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if not fields or fields[0].upper() != "FROM":
            continue
        if len(fields) < 2 or not TAGGED_IMAGE.fullmatch(fields[1]):
            raise SystemExit(f"Expected a literal base image tag in {dockerfile}: {line}")
        images.append(fields[1])
    if not images:
        raise SystemExit(f"No base images found in {dockerfile}.")
    return images


def matches_platform(descriptor, platform):
    return f"{descriptor.get('os')}/{descriptor.get('architecture')}" == platform


def checked_digest(value, image):
    if not isinstance(value, str) or not DIGEST.fullmatch(value):
        raise SystemExit(f"Invalid digest for {image}.")
    return value


def base_digest(image, platform):
    manifest = inspect(image, "--raw")
    if "manifests" in manifest:
        for entry in manifest["manifests"]:
            if matches_platform(entry.get("platform") or {}, platform):
                return checked_digest(entry.get("digest"), image)
    else:
        details = inspect(image, "--format", "{{json .}}")
        if matches_platform(details.get("image") or {}, platform):
            return checked_digest((details.get("manifest") or {}).get("digest"), image)
    raise SystemExit(f"No {platform} digest found for {image}.")


def main():
    platform = os.environ.get("TARGET_PLATFORM", "linux/amd64")
    names = base_images(Path("release-source/Dockerfile"))
    bases = {name: base_digest(name, platform) for name in dict.fromkeys(names)}
    for name, digest in bases.items():
        print(f"{platform} base image: {name}@{digest}")

    inputs = json.dumps([os.environ["RELEASE_REVISION"], platform, list(bases.items())])
    fingerprint = hashlib.sha256(inputs.encode()).hexdigest()
    release_image = f"{os.environ['IMAGE']}:{os.environ['RELEASE_TAG']}"
    published = inspect(release_image, "--format", "{{json .}}", allow_missing=True)

    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
        if published is not None:
            image = published.get("image") or {}
            image = image.get(platform, image)
            labels = (image.get("config") or {}).get("Labels") or {}
            if matches_platform(image, platform) and labels.get(BUILD_INPUTS_LABEL) == fingerprint:
                digest = checked_digest((published.get("manifest") or {}).get("digest"), release_image)
                print(f"reusable_digest={digest}", file=output)
                print(f"Reusing {release_image} with unchanged build inputs.")
                return

        runtime = names[-1]
        print(f"base_name={runtime}", file=output)
        print(f"base_digest={bases[runtime]}", file=output)
        print(f"fingerprint={fingerprint}", file=output)
        print("contexts<<BASE_IMAGES", file=output)
        for name, digest in bases.items():
            print(f"{name}=docker-image://{name}@{digest}", file=output)
        print("BASE_IMAGES", file=output)


if __name__ == "__main__":
    main()
