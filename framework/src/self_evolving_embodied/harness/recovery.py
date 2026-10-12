"""Conservative bounded recovery: MVP never retries an unknown/non-idempotent action."""
class RecoveryManager:
    @staticmethod
    def decision(*, failure_code: str, retries_used: int, max_retries: int,
                 side_effect_unknown: bool = False) -> str:
        if side_effect_unknown:
            return "HUMAN_REVIEW"
        if retries_used >= max_retries:
            return "ABORT"
        # Retry executor is intentionally deferred to Step 7.
        return "ABORT"
