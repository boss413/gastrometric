import { Component, type ReactNode } from "react";

interface ErrorBoundaryProps {
  children: ReactNode;
}

interface ErrorBoundaryState {
  hasError: boolean;
}

/**
 * Minimal application-level error boundary (FE-01 §1.7 / work order §17).
 * Renders a plain, human-readable message. Never exposes a stack trace.
 */
export class ErrorBoundary extends Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  state: ErrorBoundaryState = { hasError: false };

  static getDerivedStateFromError(): ErrorBoundaryState {
    return { hasError: true };
  }

  componentDidCatch(error: unknown): void {
    console.error(error);
  }

  render(): ReactNode {
    if (this.state.hasError) {
      return (
        <div className="app-error" role="alert">
          <h1>Something went wrong</h1>
          <p>Please reload the page and try again.</p>
        </div>
      );
    }

    return this.props.children;
  }
}
