import { useNavigate } from "react-router-dom";

interface BackButtonProps {
  /** Route to go to when there is no in-app history to go back to (e.g. a deep link). */
  fallbackTo: string;
  /** Destination name shown in the label, e.g. "Back to Recipes". */
  label: string;
}

/**
 * Shared Back control. Prefers real browser back navigation so device/browser
 * Back stays consistent with in-app Back; falls back to a fixed route when
 * there is no prior in-app history entry to return to.
 */
export function BackButton({ fallbackTo, label }: BackButtonProps) {
  const navigate = useNavigate();

  function handleClick() {
    const historyState = window.history.state as { idx?: number } | null;
    const hasInAppHistory =
      typeof historyState?.idx === "number" && historyState.idx > 0;

    if (hasInAppHistory) {
      navigate(-1);
    } else {
      navigate(fallbackTo);
    }
  }

  return (
    <button type="button" className="back-button" onClick={handleClick}>
      ← {label}
    </button>
  );
}
