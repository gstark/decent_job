# AGENTS.md

DecentJob is a PostgreSQL job runner. It has a native job API (`DecentJob::Base`) and an optional Active Job adapter. Its only runtime dependency is `pg`. Active Job and Active Support must stay optional: the core must boot and run without them.

## Commands

- Run the specs: `bundle exec rake spec`
- Run one spec file: `bundle exec ruby -Ilib -Ispec spec/advisory_claim_spec.rb`
- Lint: `bundle exec standardrb`

## Where to look

- `lib/decent_job.rb` is the entry point and the top-level module API.
- `docs/` has one file per feature. Read the matching doc before you change a subsystem.
- `db/migrations/` has the SQL schema. Table and column names in comments must match it.

## Code documentation

Every class and module has a header comment. Public methods have YARD tags. The comments are for two readers: a human maintainer and a future coding agent. The agent cannot ask questions, so the comments must state what the code cannot show.

### Class and module headers

Write a short prose header above each class or module. Include each item below that applies:

- **Role.** One sentence that states what the class does in the system.
- **Collaborators.** Which classes create it, call it, or own it. Use YARD links, for example `{Worker}` or `{Claim#with_next}`.
- **Ownership.** Which thread, database connection, lock, or process owns the object or its state.
- **Invariants.** Rules that must stay true, for example "the advisory lock is held for the whole attempt".
- **Constraints.** Why the obvious alternative is wrong, for example "do not return this connection to the pool".
- **Doc link.** The matching file in `docs/`, if one exists.

Skip items that do not apply. Do not pad.

### Methods

Public methods get a summary line and the tags that apply:

- `@param name [Type] meaning` for each parameter. State units (seconds, milliseconds) and the meaning of `nil`.
- `@return [Type] meaning`. Say what `nil` or `false` means.
- `@yield` / `@yieldparam` / `@yieldreturn` for blocks.
- `@raise [ErrorClass] condition` for errors the caller must handle.
- `@note` for thread safety, side effects (SQL writes, NOTIFY, signals, forks), or ordering rules.
- `@api private` on public methods that are internal to the gem.

Private methods get a comment only when the behavior is not obvious: locks, races, interrupt handling, SQL tricks, or fork and signal safety. One or two lines is usually enough.

### Rules

- Explain why, not what. Do not restate the code.
- Use short sentences in the active voice.
- Use the same term for the same thing everywhere: "job", "execution", "attempt", "claim", "worker", "process", "batch", "cron entry".
- Do not describe behavior that the code does not have. If you are not sure, read the callers and the specs.
- Do not change code when you add documentation.
- Keep existing comments that explain non-obvious behavior. Merge them into the new header if they belong there.
- Run `bundle exec standardrb` after you edit.

### Example

```ruby
# Claims one runnable job and holds the claim for the whole attempt.
#
# {Worker} execution threads call {#with_next} in a loop. The claim strategy
# comes from {Configuration#lock_strategy}: an advisory lock, a SKIP LOCKED
# row claim, or a hybrid of both. The thread keeps its database checkout until
# the attempt ends, because a session advisory lock belongs to the connection.
#
# See docs/locking.md.
class Claim
  # Claims the next runnable job and yields it.
  #
  # @param queues [String, QueueSelection] queue selection, "*" for all queues
  # @param job_id [String, nil] claim only this job, or any job when nil
  # @param stopping [#call] returns true when the caller must stop claiming
  # @yieldparam row [Hash{String => Object}] the claimed jobs row
  # @return [Object, nil] the block result, or nil when no job was claimed
  # @note The claim is released in an ensure block, also when the block raises.
  def with_next(queues: "*", job_id: nil, stopping: -> { false })
```
