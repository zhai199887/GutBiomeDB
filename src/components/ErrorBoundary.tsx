import { Component, type ErrorInfo, type ReactNode } from "react";

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  error?: Error;
}

class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false };

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("ErrorBoundary caught:", error, info.componentStack);
  }

  handleReset = () => {
    this.setState({ hasError: false, error: undefined });
  };

  render() {
    if (!this.state.hasError) return this.props.children;

    return (
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          minHeight: "60vh",
          gap: "1.5rem",
          padding: "2rem",
          textAlign: "center",
          color: "#e4e8ee",
        }}
      >
        <h1 style={{ fontSize: "1.8rem", fontWeight: 600, margin: 0 }}>
          Something went wrong
        </h1>
        <p style={{ opacity: 0.6, maxWidth: 400 }}>
          An unexpected error occurred. Please try again or return to the home page.
        </p>
        {import.meta.env.DEV && this.state.error && (
          <pre
            style={{
              maxWidth: "80vw",
              padding: "1rem",
              background: "rgba(255,255,255,0.05)",
              borderRadius: 8,
              fontSize: "0.8rem",
              overflow: "auto",
              textAlign: "left",
            }}
          >
            {this.state.error.message}
          </pre>
        )}
        <div style={{ display: "flex", gap: "1rem" }}>
          <button
            onClick={this.handleReset}
            style={{
              padding: "0.5rem 1.5rem",
              background: "#e23fff",
              color: "#fff",
              border: "none",
              borderRadius: 6,
              cursor: "pointer",
              fontSize: "0.95rem",
            }}
          >
            Retry
          </button>
          <a
            href="/"
            style={{
              padding: "0.5rem 1.5rem",
              border: "1px solid rgba(255,255,255,0.3)",
              borderRadius: 6,
              color: "#e4e8ee",
              textDecoration: "none",
              fontSize: "0.95rem",
            }}
          >
            Home
          </a>
        </div>
      </div>
    );
  }
}

export default ErrorBoundary;
