import json, os, re, sys
src, outdir = sys.argv[1], sys.argv[2]
os.makedirs(outdir, exist_ok=True)
lines = open(src, encoding='utf-8').read().split('\n')
names = {}
i = 0
while i < len(lines):
    if not lines[i].startswith('Response example:'):
        i += 1
        continue
    j, depth, buf = i + 1, 0, []
    while j < len(lines):
        buf.append(lines[j])
        depth += lines[j].count('{') + lines[j].count('[') - lines[j].count('}') - lines[j].count(']')
        j += 1
        if depth <= 0 and buf and buf[0].strip():
            break
    raw = re.sub(r'[\x00-\x08\x0b-\x1f]', '', '\n'.join(buf))
    doc = json.loads(raw)
    name = None
    for k in range(j, min(j + 40, len(lines))):
        m = re.match(r'^(GET|POST|PUT|DELETE) /(public|private)/v1/(\S+)', lines[k])
        if m:
            name = f"{m.group(2)}_{m.group(1).lower()}_{m.group(3).replace('/', '_')}"
            break
        if lines[k].startswith('Response example:'):
            break
    if name is None and isinstance(doc, dict) and 'channel' in doc:
        prefix = 'ws_private' if doc['channel'].endswith('Events') else 'ws_public'
        name = f"{prefix}_{doc['channel']}"
    names[name] = names.get(name, 0) + 1
    if names[name] > 1:
        name = f"{name}_{names[name]}"
    with open(os.path.join(outdir, f'{name}.json'), 'w', encoding='utf-8') as f:
        f.write(raw.strip() + '\n')
    i = j
print(len(sum([[n] * c for n, c in names.items()], [])), 'fixtures')
for n, c in sorted(names.items()):
    print(c, n)
