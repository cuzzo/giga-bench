# giga-bench

`delivery`: a durable message queue that must survive disk errors mid-write, retries and crashes.

 * The `original/` was written by DeepSeek over requests 1-4.
 * Gemini 4 Argon's simplification of it under three different prompts.

## The Three Prompts

 1. Generic: told to simplify. 
 2. Clones: also given clone data. 
 3. Giga: also given Giga's analysis. 

## The solutions

Each Argon file is the run nearest its arm's median over 5 seeds. No arm failed a test the original passes (including the heldout tests).

> [!NOTE] 
> Median is chosen purely based on bug-bit and statement reduction, not an attempt to subjectively judge which of these was median "readable" / "understandable".

| Folder | Prompt | Statements | Bug bits | Artificial statements | Artificial bug bits |
| --- | --- | --- | --- | --- | --- |
| `original/` | - | 804 | 181.6 | 562 | 138.6 |
| `argon/generic-seed1/` | simplify | 573 | 143.3 | 331 | 100.3 |
| `argon/clones-seed4/` | simplify, with clone data | 475 | 120.5 | 233 | 77.5 |
| `argon/giga-seed4/` | simplify, with Giga's analysis | 361 | 65.1 | 119 | 22.1 |

Artificial = the amount above an "ideal" solution that passes the same checks (242 statements, 43.0 bug bits).

> [!NOTE] 
> There are known solutions with less statements and less bug-bits.  The ideal solution is based on my subjective "readable" / "understable" opinion.

`original/test.mjs` is the developer test suite; the requests are the specifications the original was built from.
