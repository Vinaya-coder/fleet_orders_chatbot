"""
run.py  —  Test both agents side by side with the same query.

Usage:
    python run.py
    python run.py "How many orders failed yesterday?"
"""

import sys
import time

# ── Change this to any question you want to test ─────────────────────────────
DEFAULT_QUERY = "How many multi-orders had extraction failures yesterday?"
# ─────────────────────────────────────────────────────────────────────────────


def print_section(title: str, char: str = "─", width: int = 60):
    print(f"\n{char * width}")
    print(f"  {title}")
    print(char * width)


def print_result(label: str, result: dict, elapsed: float):
    print(f"\n▶  {label}  ({elapsed:.1f}s)")
    print(f"\n   Answer : {result['answer']}")
    print(f"\n   Steps  : {len(result['steps'])} tool call(s)")

    for i, step in enumerate(result['steps'], 1):
        tool_name = step.get("tool", "?")
        args = step.get("args", {})
        sql = args.get("sql", "")
        label_str = f"     [{i}] {tool_name}"
        if sql:
            first_line = sql.strip().splitlines()[0][:80]
            print(f"{label_str}  ->  {first_line}...")
        else:
            print(f"{label_str}")

    if result.get("error"):
        print(f"\n   WARNING: {result['error']}")

    if result.get("sql"):
        print(f"\n   SQL:\n   {result['sql'][:300]}")

    if result.get("rows") is not None:
        print(f"\n   Rows returned: {len(result['rows'])}")


def main():
    query = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_QUERY

    print(f"\n{'='*64}")
    print(f"  Query: {query}")
    print(f"{'='*64}")

    # Run pure agent
    print("\n[1/2] Running pure agent...")
    from agent_pure import run_agent as pure_agent
    t0 = time.time()
    pure_result = pure_agent(query)
    pure_elapsed = time.time() - t0
    print_result("PURE AGENT", pure_result, pure_elapsed)

    # Run guarded agent
    print("\n[2/2] Running guarded agent...")
    from agent_guarded import run_agent as guarded_agent
    t0 = time.time()
    guarded_result = guarded_agent(query)
    guarded_elapsed = time.time() - t0
    print_result("GUARDED AGENT", guarded_result, guarded_elapsed)

    # Summary
    print_section("SUMMARY", "=")
    print(f"\n  {'':20s}  {'Pure':>10}  {'Guarded':>10}")
    print(f"  {'-'*44}")
    print(f"  {'Time (s)':20s}  {pure_elapsed:>10.1f}  {guarded_elapsed:>10.1f}")
    print(f"  {'Tool calls':20s}  {len(pure_result['steps']):>10}  {len(guarded_result['steps']):>10}")
    print(f"  {'SQL executed':20s}  {'yes' if pure_result.get('sql') else 'no':>10}  {'yes' if guarded_result.get('sql') else 'no':>10}")
    print(f"  {'Error':20s}  {'yes' if pure_result.get('error') else 'no':>10}  {'yes' if guarded_result.get('error') else 'no':>10}")
    print()


if __name__ == "__main__":
    main()