import { motion } from "motion/react";
import { useId, useLayoutEffect, useRef, useState } from "react";

export interface DiagramEdge {
  active?: boolean;
  color?: string;
  from: string;
  to: string;
}
interface Point {
  x: number;
  y: number;
}
type Connection = DiagramEdge & { path: string; start: Point; end: Point };

type Side = "left" | "right" | "top" | "bottom";
function nodePoint(node: Element, side: Side, root: DOMRect): Point {
  const box = node.getBoundingClientRect();
  let x = box.left + box.width / 2;
  let y = box.top + box.height / 2;
  if (side === "left") {
    x = box.left;
  }
  if (side === "right") {
    x = box.right;
  }
  if (side === "top") {
    y = box.top;
  }
  if (side === "bottom") {
    y = box.bottom;
  }
  return { x: x - root.left, y: y - root.top };
}
function connectionPath(
  start: Point,
  end: Point,
  vertical: boolean,
  workerBranch: boolean
) {
  if (workerBranch) {
    // The mobile worker bus occupies a reserved gutter, never another card.
    const bus = end.x - 18;
    const turn = start.y + 18;
    return `M${start.x},${start.y} V${turn} H${bus} V${end.y} H${end.x}`;
  }
  if (vertical) {
    const mid = (start.y + end.y) / 2;
    return `M${start.x},${start.y} C${start.x},${mid} ${end.x},${mid} ${end.x},${end.y}`;
  }
  const mid = (start.x + end.x) / 2;
  return `M${start.x},${start.y} C${mid},${start.y} ${mid},${end.y} ${end.x},${end.y}`;
}
function measureConnections(
  container: HTMLElement,
  edges: DiagramEdge[]
): Connection[] {
  const root = container.getBoundingClientRect();
  const vertical =
    getComputedStyle(container)
      .getPropertyValue("--diagram-direction")
      .trim() === "vertical";
  const nodes = new Map(
    [...container.querySelectorAll<HTMLElement>("[data-node]")].map((node) => [
      node.dataset.node,
      node,
    ])
  );
  return edges.flatMap((edge) => {
    const from = nodes.get(edge.from);
    const to = nodes.get(edge.to);
    if (!(from && to)) {
      return [];
    }
    const workerBranch = vertical && edge.to.startsWith("worker-");
    const source =
      vertical && edge.from.startsWith("tenant-")
        ? (from.closest(".tenant") ?? from)
        : from;
    const start = nodePoint(source, vertical ? "bottom" : "right", root);
    const end = nodePoint(to, workerBranch || !vertical ? "left" : "top", root);
    return [
      {
        ...edge,
        end,
        path: connectionPath(start, end, vertical, workerBranch),
        start,
      },
    ];
  });
}

// Measure the same DOM nodes that the visitor sees. Fixed SVG coordinates drift
// when fonts load, labels wrap, the viewport changes, or workers are added.
export function DiagramEdges({
  edges,
  playing = false,
}: {
  edges: DiagramEdge[];
  playing?: boolean;
}) {
  const ref = useRef<SVGSVGElement>(null);
  const marker = useId().replaceAll(":", "");
  const [connections, setConnections] = useState<Connection[]>([]);
  const edgeKey = JSON.stringify(edges);
  useLayoutEffect(() => {
    const svg = ref.current;
    const container = svg?.parentElement;
    if (!container) {
      return;
    }
    const currentEdges: DiagramEdge[] = JSON.parse(edgeKey);
    let disposed = false;
    const measure = () => {
      if (disposed) {
        return;
      }
      const measured = measureConnections(container, currentEdges);
      setConnections((previous) =>
        JSON.stringify(previous) === JSON.stringify(measured)
          ? previous
          : measured
      );
    };
    const observer = new ResizeObserver(measure);
    observer.observe(container);
    for (const node of container.querySelectorAll("[data-node]")) {
      observer.observe(node);
    }
    // Font substitution can reposition nodes without changing the container size.
    document.fonts.ready.then(measure);
    window.addEventListener("resize", measure);
    measure();
    return () => {
      disposed = true;
      observer.disconnect();
      window.removeEventListener("resize", measure);
    };
  }, [edgeKey]);
  return (
    <svg aria-hidden="true" className="diagram-edges" ref={ref}>
      <defs>
        <marker
          id={marker}
          markerHeight="5"
          markerWidth="5"
          orient="auto-start-reverse"
          refX="6"
          refY="3"
          viewBox="0 0 6 6"
        >
          <path d="m1 1 4 2-4 2" fill="none" stroke="#9aafc0" strokeWidth="1" />
        </marker>
      </defs>
      {connections.map((edge) => (
        <g key={`${edge.from}-${edge.to}`}>
          <path
            className="diagram-edge"
            d={edge.path}
            data-from={edge.from}
            data-to={edge.to}
            markerEnd={`url(#${marker})`}
          />
          {edge.active === true && playing && (
            <motion.path
              animate={{ strokeDashoffset: [1, 0] }}
              className="diagram-flow"
              d={edge.path}
              pathLength={1}
              stroke={edge.color ?? "#699fca"}
              strokeDasharray=".08 .92"
              transition={{
                duration: 2,
                ease: "linear",
                repeat: Number.POSITIVE_INFINITY,
              }}
            />
          )}
        </g>
      ))}
    </svg>
  );
}
