//! The Lua the package sends with `EVAL` for compare-and-swap.

/// The compare-and-swap script, verbatim from the native `omnia-redis` crate.
///
/// Called with one key and three arguments: `1` or `0` for whether an
/// expected value follows, the expected value, and the new value. It replies
/// with `{swapped, present, observed}`: `swapped` is `1` when the value was
/// written, `present` is `1` when the key held a value that did not match,
/// and `observed` is that value.
pub const CAS_SCRIPT: &str = r"
local current = redis.call('GET', KEYS[1])
local matches
if ARGV[1] == '1' then
  matches = current ~= false and current == ARGV[2]
else
  matches = current == false
end
if matches then
  redis.call('SET', KEYS[1], ARGV[3], 'KEEPTTL')
  return {1, 0, ''}
end
if current == false then
  return {0, 0, ''}
end
return {0, 1, current}
";
