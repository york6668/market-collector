"""Telegram length budgeting in UTF-16 units, preserving report section boundaries."""
def utf16_length(text):
    return len(text.encode("utf-16-le")) // 2


def shorten_line(text, budget):
    if utf16_length(text) <= budget:
        return text
    result, used = [], 0
    for char in text:
        size = utf16_length(char)
        if used + size > max(0, budget - 1):
            break
        result.append(char)
        used += size
    return "".join(result) + "…"


def render_report(lines, budget=3800):
    blocks, current = [], []
    for line in lines:
        if line == "":
            if current:
                blocks.append(current)
                current = []
        else:
            current.append(str(line))
    if current:
        blocks.append(current)

    def render():
        return "\n\n".join("\n".join(block) for block in blocks)

    if utf16_length(render()) <= budget:
        return render()

    # News yields its space first. Critical interpretation and quality remain intact.
    for block in blocks:
        if block[0].startswith("📰"):
            block[1:] = ["新闻已按消息长度缩减。"]
    if utf16_length(render()) <= budget:
        return render()

    protected = lambda b: any(b[0].startswith(prefix) for prefix in (
        "📊", "💰", "🎯", "🧭", "🤖", "📎"))
    for limit in (220, 160, 110, 80):
        for block in blocks:
            if not protected(block):
                block[1:] = [shorten_line(line, limit) for line in block[1:]]
        if utf16_length(render()) <= budget:
            return render()

    # Bound each module rather than cutting the report tail and dropping quality.
    for maximum_lines in (4, 3, 2):
        for block in blocks:
            if not protected(block) and len(block) > maximum_lines:
                block[maximum_lines:] = []
                block[-1] = shorten_line(block[-1], 65) + "（本节已缩减）"
        if utf16_length(render()) <= budget:
            return render()

    # Extraordinary input: retain every heading and prioritize the protected modules.
    for block in blocks:
        if not protected(block):
            block[1:] = ["本节内容超长，详见完整报告。"]
    if utf16_length(render()) <= budget:
        return render()
    for limit in (160, 110, 80, 45):
        for block in blocks:
            block[:] = [shorten_line(line, limit) for line in block]
        if utf16_length(render()) <= budget:
            return render()
    # A report with too many essential lines cannot be sent faithfully in one message.
    raise ValueError("report_essential_sections_exceed_telegram_budget")
