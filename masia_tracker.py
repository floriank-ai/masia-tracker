"""
CASA MUSA - Masia Alert
Scraping via Claude API mit realistischen Filtern.
3 Kategorien: Masias, Terrenos, Bonus (nah dran aber interessant).
"""

import json
import os
import re
import smtplib
import ssl
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

CONFIG = {
    "email_absender":   os.environ.get("EMAIL_ABSENDER",   ""),
    "email_passwort":   os.environ.get("EMAIL_PASSWORT",   ""),
    "email_empfaenger": os.environ.get("EMAIL_EMPFAENGER", ""),
    "anthropic_key":    os.environ.get("ANTHROPIC_API_KEY", ""),
    "smtp_server":      "smtp.gmail.com",
    "smtp_port":        587,
    "archivo_vistos":   "masia_gesehen.json",
}

DELAY_ENTRE_REQUESTS = 75

PORTALES = [
    {
        "nombre": "Buscomasia",
        "urls": [
            "https://www.buscomasia.com/venta/provincia-tarragona/precio-asc/",
            "https://www.buscomasia.com/priorat/",
        ],
    },
    {
        "nombre": "Milanuncios",
        "urls": [
            "https://www.milanuncios.com/fincas-rusticas-en-tarragona/",
        ],
    },
    {
        "nombre": "Fotocasa (Via Augusta)",
        "urls": [
            "https://www.fotocasa.es/es/inmobiliaria-finques-via-augusta/comprar/inmuebles/espana/todas-las-zonas/l?clientId=9202754898213",
        ],
    },
]


PROMPT_EXTRACCION = """Du bekommst eine spanische Immobilien-Webseite. Extrahiere ABSOLUT JEDE EINZELNE Immobilien-Anzeige.

KRITISCH: Extrahiere JEDE Anzeige, auch wenn es 30, 50 oder 100 sind. NICHT nur 2-3!

Gib JSON zurueck: {"anuncios": [{"ref": "...", "titulo": "...", "pueblo": "...", "comarca": "...", "precio": 90000, "precio_original": null, "m2_construida": null, "m2_parcela": 55000, "url": "https://...", "estado": "disponible", "tipo": "masia"}]}

Felder:
- ref: Referenznummer/ID
- titulo: Titel
- pueblo: Ortschaft
- comarca: Region (Baix Ebre, Priorat, Alt/Baix Camp, Alt/Baix Penedès, Ribera d'Ebre, Terra Alta, Tarragonès, Montsià)
- precio: aktueller Preis in Euro als Zahl (90000 nicht "90.000 €")
- precio_original: Originalpreis bei Rabatt, sonst null
- m2_construida: Haus-m² als Zahl, null wenn keine Info
- m2_parcela: Grundstueck-m² als Zahl (parcela/terreno/plot)
- url: volle URL
- estado: disponible | reservado | vendido | novedad | oportunidad | rebajado
- tipo: masia (mit Haus/Gebaeude/Casa) | terreno (nur Grundstueck)

NUR JSON, kein Markdown, keine ``` Marker.
"""


def scrape_via_claude(url, portal_name, retry=0):
    if not CONFIG["anthropic_key"]:
        print("  ⚠️  ANTHROPIC_API_KEY no configurada")
        return []

    headers = {
        "x-api-key": CONFIG["anthropic_key"],
        "anthropic-version": "2023-06-01",
        "anthropic-beta": "web-fetch-2025-09-10",
        "content-type": "application/json",
    }

    body = {
        "model": "claude-sonnet-4-6",
        "max_tokens": 16000,
        "tools": [{
            "type": "web_fetch_20250910",
            "name": "web_fetch",
            "max_uses": 1,
        }],
        "messages": [{
            "role": "user",
            "content": f"{PROMPT_EXTRACCION}\n\nRufe ab und extrahiere ALLE Anzeigen: {url}"
        }]
    }

    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
                          headers=headers, json=body, timeout=240)
        if r.status_code == 429 and retry < 3:
            print(f"      ⏳ Rate limit, warte 70s... (retry {retry+1}/3)")
            time.sleep(70)
            return scrape_via_claude(url, portal_name, retry + 1)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        print(f"      ❌ API error: {str(e)[:150]}")
        return []

    text_parts = []
    for block in data.get("content", []):
        if block.get("type") == "text":
            text_parts.append(block.get("text", ""))
    full_text = "\n".join(text_parts)

    full_text = re.sub(r'```(?:json)?\s*', '', full_text)
    full_text = re.sub(r'```', '', full_text)

    json_match = re.search(r'\{[\s\S]*"anuncios"[\s\S]*\}', full_text)
    if not json_match:
        print(f"      ⚠️  Kein JSON")
        return []

    try:
        parsed = json.loads(json_match.group(0))
        anuncios = parsed.get("anuncios", [])
        for a in anuncios:
            a["fuente"] = portal_name
        return anuncios
    except json.JSONDecodeError:
        return []


def recopilar_todos():
    todos = {}
    primer = True
    for portal in PORTALES:
        print(f"\n  📡 {portal['nombre']}")
        for url in portal["urls"]:
            if not primer:
                print(f"    ⏸  Pausa {DELAY_ENTRE_REQUESTS}s...")
                time.sleep(DELAY_ENTRE_REQUESTS)
            primer = False
            print(f"    → {url[:80]}")
            anuncios = scrape_via_claude(url, portal["nombre"])
            print(f"      -> {len(anuncios)} anuncios")
            for a in anuncios:
                ref = str(a.get("ref", "")).strip() or a.get("url", "")[-30:]
                a_id = f"{portal['nombre']}_{ref}".replace(" ", "_")
                if a_id not in todos:
                    a["id"] = a_id
                    todos[a_id] = a
    return list(todos.values())


def cargar_vistos():
    try:
        with open(CONFIG["archivo_vistos"]) as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def guardar_vistos(vistos):
    with open(CONFIG["archivo_vistos"], "w") as f:
        json.dump(sorted(vistos), f, ensure_ascii=False, indent=2)


def m2_a_ha(m2):
    if m2 is None:
        return None
    try:
        return round(float(m2) / 10_000, 2)
    except (ValueError, TypeError):
        return None


def clasificar(a):
    tipo = (a.get("tipo") or "").lower().strip()
    if tipo in ("masia", "terreno"):
        return tipo
    t = (a.get("titulo") or "").lower()
    if any(p in t for p in ["masia", "masía", "casa", "finca con", "vivienda", "chalet", "xalet", "hotel", "con vivienda", "con casa"]):
        return "masia"
    return "terreno"


# ============ NEUE KATEGORISIERUNG: perfect / bonus / nein ============

def categorizar(a):
    """
    Returns: ('perfecto', reason) | ('bonus', reason) | ('descartado', reason)

    PERFEKT = erfuellt alle Kriterien strict:
      - Masia: max 150k€, min 5ha
      - Terreno: max 60k€, min 10ha

    BONUS = "nah dran" oder besonders interessant:
      - Masia: max 200k€ ODER min 2ha
      - Terreno: grosse Flaeche (20ha+) auch wenn teurer
      - Oder: guter Preis/Flaeche Ratio
    """
    estado = (a.get("estado") or "").lower().strip()
    if estado == "vendido":
        return "descartado", "vendido"

    precio = a.get("precio")
    if precio is None or precio <= 0:
        return "descartado", "sin precio"

    tipo = clasificar(a)
    m2p = a.get("m2_parcela")
    ha = m2_a_ha(m2p) if m2p else None

    if tipo == "masia":
        # PERFEKT
        if precio <= 150_000 and (ha is None or ha >= 5):
            return "perfecto", f"masia {precio}€ {ha}ha"
        # BONUS - nah dran
        if precio <= 200_000 and (ha is None or ha >= 2):
            return "bonus", f"masia {precio}€ {ha}ha (cerca)"
        # Ausgeschlossen
        if precio > 200_000:
            return "descartado", f"muy caro {precio}€"
        return "descartado", f"poca superficie {ha}ha"

    if tipo == "terreno":
        # PERFEKT
        if precio <= 60_000 and (ha is None or ha >= 10):
            return "perfecto", f"terreno {precio}€ {ha}ha"
        # BONUS - sehr viel Flaeche oder bezahlbar-groß
        if ha is not None and ha >= 20 and precio <= 150_000:
            return "bonus", f"terreno grande {ha}ha"
        if precio <= 90_000 and (ha is None or ha >= 5):
            return "bonus", f"terreno {precio}€ {ha}ha (cerca)"
        # Ausgeschlossen
        if precio > 150_000:
            return "descartado", f"muy caro {precio}€"
        return "descartado", f"poca superficie"

    return "descartado", "?"


# ============ EMAIL ============

def fp(p):
    if p is None or p == 0: return "consultar"
    return f"{int(p):,} €".replace(",", ".")

def fh(m2):
    if not m2: return "? ha"
    ha = float(m2) / 10_000
    return f"{ha:.1f} ha".replace(".", ",")

def fm(m):
    if not m: return "—"
    return f"{int(m):,} m²".replace(",", ".")


def badge_estado(estado):
    if not estado: return ""
    e = estado.lower()
    colors = {
        "novedad":    ("#2d8a4f", "NUEVO"),
        "rebajado":   ("#c74b2e", "REBAJADO"),
        "oportunidad":("#d97706", "OPORTUNIDAD"),
        "reservado":  ("#6b7280", "RESERVADO"),
    }
    if e in colors:
        bg, text = colors[e]
        return f'<span style="background:{bg}; color:white; font-size:10px; padding:2px 6px; border-radius:3px; font-weight:600; margin-left:6px;">{text}</span>'
    return ""


def render_tarjeta(a, es_bonus=False):
    tipo = clasificar(a)
    label = "🏠 MASIA" if tipo == "masia" else "🌾 TERRENO"
    color = "#8B4513" if tipo == "masia" else "#556B2F"

    # Bonus-Markierung
    bonus_badge = ""
    if es_bonus:
        bonus_badge = '<span style="background:#f59e0b; color:white; font-size:10px; padding:2px 6px; border-radius:3px; font-weight:600; margin-left:6px;">CERCA</span>'

    ubi = a.get("pueblo") or ""
    if a.get("comarca"):
        ubi += f" · {a['comarca']}"
    if not ubi: ubi = "Tarragona"

    precio_html = f'<div style="color:#c74b2e; font-size:20px; font-weight:700;">{fp(a.get("precio"))}</div>'
    if a.get("precio_original") and a["precio_original"] != a.get("precio"):
        precio_html += f'<div style="color:#999; font-size:12px; text-decoration:line-through;">{fp(a["precio_original"])}</div>'

    ref = a.get("ref", "")
    url_prop = a.get("url") or "#"

    return f"""
    <tr><td style="padding:18px 24px; border-bottom:1px solid #e5e5e5;">
      <div style="margin-bottom:10px;">
        <span style="background:{color}; color:white; font-size:11px; padding:3px 8px; border-radius:3px; font-weight:600; letter-spacing:0.5px;">{label}</span>
        {bonus_badge}
        {badge_estado(a.get("estado"))}
        <span style="color:#888; font-size:11px; margin-left:8px;">{a.get("fuente", "")}{" · Ref. " + str(ref) if ref else ""}</span>
      </div>
      <div style="margin-bottom:6px;">
        <a href="{url_prop}" style="color:#1a1a1a; font-size:16px; font-weight:600; text-decoration:none; line-height:1.3;">{a.get('titulo', 'Sin título')}</a>
      </div>
      <div style="color:#666; font-size:13px; margin-bottom:12px;">📍 {ubi}</div>
      <table cellspacing="0" cellpadding="0" style="margin-bottom:12px;">
        <tr>
          <td style="padding-right:24px;">
            <div style="color:#999; font-size:10px; text-transform:uppercase; letter-spacing:0.5px;">Precio</div>
            {precio_html}
          </td>
          <td style="padding-right:24px;">
            <div style="color:#999; font-size:10px; text-transform:uppercase; letter-spacing:0.5px;">Parcela</div>
            <div style="color:#1a1a1a; font-size:15px; font-weight:600;">{fh(a.get("m2_parcela"))}</div>
            <div style="color:#888; font-size:11px;">{fm(a.get("m2_parcela"))}</div>
          </td>
          <td>
            <div style="color:#999; font-size:10px; text-transform:uppercase; letter-spacing:0.5px;">Construido</div>
            <div style="color:#1a1a1a; font-size:15px; font-weight:600;">{fm(a.get("m2_construida"))}</div>
          </td>
        </tr>
      </table>
      <a href="{url_prop}" style="background:#1a1a1a; color:white; padding:8px 16px; text-decoration:none; border-radius:4px; font-size:12px; font-weight:600; display:inline-block;">Ver propiedad →</a>
    </td></tr>
    """


def construir_email(perfectos, bonus, total_scraped):
    fecha = datetime.now().strftime("%d.%m.%Y")

    p_masias   = sorted([a for a in perfectos if clasificar(a) == "masia"],   key=lambda a: a.get("precio") or 9e9)
    p_terrenos = sorted([a for a in perfectos if clasificar(a) == "terreno"], key=lambda a: a.get("precio") or 9e9)
    b_masias   = sorted([a for a in bonus     if clasificar(a) == "masia"],   key=lambda a: a.get("precio") or 9e9)
    b_terrenos = sorted([a for a in bonus     if clasificar(a) == "terreno"], key=lambda a: a.get("precio") or 9e9)

    bloques = ""

    if p_masias or p_terrenos:
        bloques += '<tr><td style="padding:20px 24px 6px; background:#1a1a1a; color:white;"><h2 style="margin:0; font-size:15px; letter-spacing:1px; text-transform:uppercase;">⭐ Cumplen todos los criterios</h2></td></tr>'
        if p_masias:
            bloques += f'<tr><td style="padding:16px 24px 8px; background:#f5f1ea;"><h3 style="margin:0; font-size:15px; color:#1a1a1a;">🏠 Masias ({len(p_masias)})</h3></td></tr>'
            bloques += "".join(render_tarjeta(a) for a in p_masias)
        if p_terrenos:
            bloques += f'<tr><td style="padding:16px 24px 8px; background:#f5f1ea;"><h3 style="margin:0; font-size:15px; color:#1a1a1a;">🌾 Terrenos ({len(p_terrenos)})</h3></td></tr>'
            bloques += "".join(render_tarjeta(a) for a in p_terrenos)

    if b_masias or b_terrenos:
        bloques += '<tr><td style="padding:20px 24px 6px; background:#fff7ed; border-top:2px solid #f59e0b;"><h2 style="margin:0; font-size:15px; letter-spacing:1px; text-transform:uppercase; color:#92400e;">🔥 Cerca de los criterios</h2><p style="margin:4px 0 0; font-size:12px; color:#78350f;">Propiedades interesantes aunque no cumplen 100%</p></td></tr>'
        if b_masias:
            bloques += f'<tr><td style="padding:16px 24px 8px; background:#fffbeb;"><h3 style="margin:0; font-size:15px; color:#1a1a1a;">🏠 Masias ({len(b_masias)})</h3></td></tr>'
            bloques += "".join(render_tarjeta(a, es_bonus=True) for a in b_masias)
        if b_terrenos:
            bloques += f'<tr><td style="padding:16px 24px 8px; background:#fffbeb;"><h3 style="margin:0; font-size:15px; color:#1a1a1a;">🌾 Terrenos ({len(b_terrenos)})</h3></td></tr>'
            bloques += "".join(render_tarjeta(a, es_bonus=True) for a in b_terrenos)

    if not perfectos and not bonus:
        bloques = f'<tr><td style="padding:40px 24px; text-align:center; color:#666;"><p style="margin:0 0 8px; font-size:15px;">Hoy no hay propiedades que cumplan los criterios.</p><p style="margin:0; font-size:13px; color:#999;">{total_scraped} scraped · 0 califican</p></td></tr>'

    total = len(perfectos) + len(bonus)
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0; padding:0; background:#efeae0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<table width="100%" cellspacing="0" cellpadding="0" style="background:#efeae0; padding:20px 0;"><tr><td align="center">
<table width="640" cellspacing="0" cellpadding="0" style="background:white; border-radius:6px; overflow:hidden;">

  <tr><td style="padding:28px 24px; background:#1a1a1a; color:white;">
    <div style="font-size:12px; letter-spacing:2px; color:#c74b2e; font-weight:600;">CASA MUSA</div>
    <h1 style="margin:6px 0 0; font-size:24px; font-weight:700;">Masia Alert</h1>
    <p style="margin:8px 0 0; font-size:13px; color:#bbb;">{fecha} · {len(perfectos)} ⭐ perfectos · {len(bonus)} 🔥 cerca</p>
  </td></tr>

  {bloques}

  <tr><td style="padding:20px 24px; background:#fafaf7; border-top:1px solid #e5e5e5;">
    <div style="color:#666; font-size:12px; line-height:1.6;">
      <strong style="color:#1a1a1a;">⭐ Criterios perfectos:</strong><br>
      🏠 Masias: max 150.000 € · min 5 ha<br>
      🌾 Terrenos: max 60.000 € · min 10 ha<br>
      <br>
      <strong style="color:#92400e;">🔥 Cerca (bonus):</strong><br>
      🏠 Masias: max 200.000 € · min 2 ha<br>
      🌾 Terrenos grandes (+20ha) hasta 150.000 €
    </div>
  </td></tr>

  <tr><td style="padding:16px 24px; background:#1a1a1a; color:#888; font-size:11px; text-align:center;">
    CASA MUSA · Alerta diaria 08:00
  </td></tr>

</table></td></tr></table></body></html>"""


def enviar_email(perfectos, bonus, nuevos, total_scraped):
    if not CONFIG["email_absender"] or not CONFIG["email_passwort"]:
        return False
    dest = [e.strip() for e in CONFIG["email_empfaenger"].split(",") if e.strip()]
    if not dest: return False

    msg = MIMEMultipart("alternative")
    np = len(perfectos)
    nb = len(bonus)
    nn = len(nuevos)
    if nn > 0:
        asunto = f"CASA MUSA · {nn} {'nueva' if nn == 1 else 'nuevas'} · {np}⭐ {nb}🔥"
    else:
        asunto = f"CASA MUSA · {np}⭐ perfectos · {nb}🔥 cerca · {datetime.now().strftime('%d.%m')}"

    msg["Subject"] = asunto
    msg["From"] = CONFIG["email_absender"]
    msg["To"] = ", ".join(dest)
    msg.attach(MIMEText(construir_email(perfectos, bonus, total_scraped), "html", "utf-8"))

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(CONFIG["smtp_server"], CONFIG["smtp_port"], timeout=30) as srv:
            srv.starttls(context=ctx)
            srv.login(CONFIG["email_absender"], CONFIG["email_passwort"])
            srv.sendmail(CONFIG["email_absender"], dest, msg.as_string())
        print(f"\n  ✉️  Email a {len(dest)}: {np}⭐ + {nb}🔥 ({nn} nuevas)")
        return True
    except Exception as e:
        print(f"  ❌ Error email: {e}")
        return False


def ejecutar():
    print("=" * 60)
    print(f"  CASA MUSA · {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 60)

    vistos = cargar_vistos()
    print(f"  Ya vistos: {len(vistos)}")

    todos = recopilar_todos()
    print(f"\n  Total scraped: {len(todos)}")

    perfectos, bonus, descartados = [], [], []
    for a in todos:
        cat, razon = categorizar(a)
        if cat == "perfecto":
            perfectos.append(a)
        elif cat == "bonus":
            bonus.append(a)
        else:
            descartados.append((a, razon))

    print(f"  ⭐ Perfectos: {len(perfectos)}")
    print(f"  🔥 Bonus (cerca): {len(bonus)}")
    print(f"  ❌ Descartados: {len(descartados)}")

    if perfectos:
        print(f"\n  ⭐ Perfectos:")
        for a in perfectos:
            print(f"    · {(a.get('titulo') or '')[:50]:50} | {a.get('precio')}€ | {m2_a_ha(a.get('m2_parcela'))}ha")

    if bonus:
        print(f"\n  🔥 Bonus:")
        for a in bonus:
            print(f"    · {(a.get('titulo') or '')[:50]:50} | {a.get('precio')}€ | {m2_a_ha(a.get('m2_parcela'))}ha")

    todos_mostrados = perfectos + bonus
    nuevos = [a for a in todos_mostrados if a["id"] not in vistos]
    print(f"\n  Nuevas: {len(nuevos)}")

    enviar_email(perfectos, bonus, nuevos, len(todos))

    for a in todos_mostrados:
        vistos.add(a["id"])
    guardar_vistos(vistos)
    print(f"  Historial: {len(vistos)}")
    print("=" * 60)


if __name__ == "__main__":
    ejecutar()
