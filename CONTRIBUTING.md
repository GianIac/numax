# Contributing to Numax

Thanks for taking a look at Numax.

I’ll keep adding small, useful issues over time to make it easier to contribute without having to understand the whole project at once.

If you want to help, a good place to start is the issues marked `good first issue`.

Small PRs are welcome.  
Docs, examples, tests, cleanup, and guest-language experiments all help.

If you have an idea, feel free to open an issue or start a discussion.  
I’ll be happy to talk !!

## Recovering release asset publication

If a published release has no assets because its workflow failed, fix the
workflow on `main`, then run:

```bash
gh workflow run release.yml --ref main -f tag=v0.1.6
```

Use the affected release's existing tag. The workflow checks that the release
is published and its tag matches the CLI version, then checks out that tag for
all builds and SBOM generation. It publishes only after all targets succeed,
including checksum signing and verification. Do not move the tag or recreate
the release just to pick up a workflow fix: rerunning the original failed run
still uses the old workflow.

Manual recovery runs use the signing identity of `release.yml@refs/heads/main`,
instead of the tag-based identity used by automatic release runs. See the
[release verification guide](https://gianiac.github.io/numax/getting-started/installation/#verify-a-release).
