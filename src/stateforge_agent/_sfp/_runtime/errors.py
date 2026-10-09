"""Define failures raised by the Agent-side SFP runtime."""


class SfpConnectionError(RuntimeError):
    """Report failure to establish or validate an SFP session."""


class SfpIncompatibleError(SfpConnectionError):
    """Report that Agent and Provider share no supported SFP version."""


class SfpManifestError(RuntimeError):
    """Report a rejected Provider manifest without exposing its contents."""
