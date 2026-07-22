# Changelog

### Breaking change: restricted deserialization ([#11](https://github.com/amzn/rheoceros/pull/11))

`intelliflow.core.serialization.loads` (and therefore `Serializable.deserialize`) now
reject serialized payloads that resolve members of process/file/network modules
(`os`, `posix`, `nt`, `subprocess`, `shutil`, `socket`, `ctypes`, `pty`, `runpy`,
`webbrowser`) or the builtins `eval`, `exec`, `compile`, `open`, `__import__`,
`breakpoint`, `input` at the *deserialization time*. This prevents a crafted dump string from executing code
**merely by being deserialized** — e.g. a payload that resolves and calls `os.system`
the moment `loads` runs.

**Who is affected:** Only serialized state that holds a *direct reference* to one of
these members — e.g. an attribute set to `os.remove` or
`functools.partial(subprocess.run, ...)`. Such payloads now raise `DeserializationError`.

**Not affected:** User functions and lambdas that merely *call* these modules inside
their body (the common case for compute/callback code) still serialize and load normally,
because they are pickled by value rather than by reference.

**Migration:** If a `DeserializationError` names a blocked module, wrap the call in a
user-defined function instead of storing the bare callable:

Then re-activate the affected application so its state is re-serialized.
