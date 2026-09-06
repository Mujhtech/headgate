import { ArrowUpRight, Check, Copy, FileText, Terminal } from "lucide-react";
import { useCallback, useState } from "react";

const installCommand = "npx skills add Mujhtech/headgate";

export function AgentSkill() {
  const [copyState, setCopyState] = useState<"idle" | "copied" | "error">(
    "idle"
  );
  const copy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(installCommand);
      setCopyState("copied");
    } catch {
      setCopyState("error");
    }
  }, []);
  const messages = {
    copied: "Install command copied.",
    error: "Select the command above to copy it manually.",
    idle: "Install with the Skills CLI in your project.",
  };

  return (
    <section
      aria-labelledby="agent-skill-title"
      className="agent-skill"
      id="agent-skill"
    >
      <div className="agent-skill-copy">
        <span className="agent-skill-label">
          <FileText aria-hidden="true" size={14} /> AGENT SKILL
        </span>
        <h3 className="agent-skill-title" id="agent-skill-title">
          Your coding agent, up to speed.
        </h3>
        <p className="agent-skill-description">
          Give your coding agent Headgate-specific guidance for producers,
          workers, resumable steps, and workflows. Includes Go and Rust
          references and operational troubleshooting.
        </p>
        <a
          className="agent-skill-link"
          href="https://github.com/Mujhtech/headgate/tree/main/skills/headgate"
        >
          Explore the skill <ArrowUpRight aria-hidden="true" size={14} />
        </a>
      </div>
      <div className="agent-skill-install">
        <div className="agent-skill-terminal">
          <div className="agent-skill-terminal-heading">
            <Terminal aria-hidden="true" size={14} /> Add to your agent’s
            toolkit
          </div>
          <div className="agent-skill-command-row">
            <span aria-hidden="true" className="agent-skill-prompt">
              $
            </span>
            <code className="agent-skill-command">{installCommand}</code>
            <button
              aria-label="Copy skill install command"
              className="agent-skill-copy-button"
              onClick={copy}
              type="button"
            >
              {copyState === "copied" ? (
                <Check aria-hidden="true" size={16} />
              ) : (
                <Copy aria-hidden="true" size={16} />
              )}
            </button>
          </div>
        </div>
        <p className="agent-skill-copy-status" role="status">
          {messages[copyState]}
        </p>
      </div>
    </section>
  );
}
