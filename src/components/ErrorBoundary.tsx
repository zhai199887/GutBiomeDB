import { Component, type ErrorInfo, type ReactNode } from "react";

import { useI18n } from "@/i18n";

interface Props {
  children: ReactNode;
  description: string;
  home: string;
  retry: string;
  title: string;
}

interface State {
  hasError: boolean;
  error?: Error;
}

class ErrorBoundaryInner extends Component<Props, State> {
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
          {this.props.title}
        </h1>
        <p style={{ opacity: 0.6, maxWidth: 400 }}>
          {this.props.description}
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
            {this.props.retry}
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
            {this.props.home}
          </a>
        </div>
      </div>
    );
  }
}

const ErrorBoundary = ({ children }: { children: ReactNode }) => {
  const { t } = useI18n();

  return (
    <ErrorBoundaryInner
      title={t("error.title")}
      description={t("error.description")}
      retry={t("error.retry")}
      home={t("error.home")}
    >
      {children}
    </ErrorBoundaryInner>
  );
};

export default ErrorBoundary;
