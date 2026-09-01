"""Define failures raised while establishing local SFP sessions."""


class SfpConnectionError(RuntimeError):
    """Report failure to establish or validate an SFP session."""


class SfpIncompatibleError(SfpConnectionError):
    """Report that Agent and Provider share no supported SFP version."""
