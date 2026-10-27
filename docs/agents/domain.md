# Domain Docs

How engineering skills should consume this repo's domain documentation.

## Before exploring, read these

- `GLOSSARY.md` at the repo root, or `GLOSSARY-MAP.md` if it exists; read the glossary for the relevant context.
- `docs/adr/` for decisions that touch the work. In a multi-context repo, also check `src/<context>/docs/adr/`.

If these files don't exist, proceed silently. The domain-modeling skill can create them when terms or decisions are resolved.

## File structure

This is a single-context repo:

    /
    ├── GLOSSARY.md
    ├── docs/adr/
    └── src/

## Use the glossary's vocabulary

Use domain terms as defined in `GLOSSARY.md`. If a needed concept is missing, note the gap for domain modeling.

## Flag ADR conflicts

If output conflicts with an ADR, name the ADR and explain why it may need to be revisited.
