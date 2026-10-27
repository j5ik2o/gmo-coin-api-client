import re, sys
src, dst = sys.argv[1], sys.argv[2]
lines = open(src, encoding='utf-8').read().split('\n')
out, skipping = [], False
for i, line in enumerate(lines):
    if line.startswith('Request example:'):
        skipping = True
        continue
    if line.startswith('Response example:'):
        skipping = False
    if not skipping:
        out.append(line)
text = '\n'.join(out)
start = text.find('\nエンドポイント\n')
end = text.find('\nフィードバック\n')
open(dst, 'w', encoding='utf-8').write(text[start:end if end > 0 else None])
