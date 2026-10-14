# giga-bench

`delivery`: a durable event store in JavaScript (`original/`, written by DeepSeek over requests 1-4), and
Gemini 4 Argon's simplification of it under three prompts. Each Argon file is the run nearest its arm's median over
5 seeds; none fails a check the original passes.

| Folder | Prompt | Statements | Bug bits |
| --- | --- | --- | --- |
| `original/` | - | 804 | 181.6 |
| `argon/generic-seed1/` | simplify | 573 | 143.3 |
| `argon/clones-seed4/` | simplify, with clone data | 475 | 120.5 |
| `argon/giga-seed4/` | simplify, with Giga's analysis | 361 | 65.1 |

`original/test.mjs` is the developer test suite; the requests are the specifications the original was built from.
