# `define "TRANSFORM ..."` in a migration reads like Ecto's own `create
# table(...)`, so the formatter leaves both without parentheses, here and,
# through `import_deps: [:trellis]`, in a host app's migrations.
locals_without_parens = [define: 1, apply: 1]

[
  import_deps: [:ecto_sql],
  inputs: ["{mix,.formatter}.exs", "{lib,test}/**/*.{ex,exs}"],
  locals_without_parens: locals_without_parens,
  export: [locals_without_parens: locals_without_parens]
]
