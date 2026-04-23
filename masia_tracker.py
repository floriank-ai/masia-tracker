"""
CASA MUSA - Masia Alert
Scraping via Gemini API (KOSTENLOS: 500 Requests/Tag im Free Tier).
Verwendet Gemini 2.5 Flash mit URL Context Tool.
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
    "gemini_key":       os.environ.get("GEMINI_API_KEY", ""),
    "smtp_server":      "smtp.gmail.com",
    "smtp_port":        587,
    "archivo_vistos":   "masia_gesehen.json",
}

# Rate limit: 10 RPM -> 7s Pause ist safe
DELAY_ENTRE_REQUESTS = 8

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
        "nombre": "Fotocasa",
        "urls": [
            "https://www.fotocasa.es/es/inmobiliaria-finques-via-augusta/comprar/inmuebles/espana/todas-las-zonas/l?clientId=9202754898213",
        ],
    },
]


PROMPT_EXTRACCION = """Analysiere die URL die dir uebergeben wird. Es ist eine spanische Immobilien-Webseite. Extrahiere ABSOLUT JEDE EINZELNE Immobilien-Anzeige.

KRITISCH: Extrahiere JEDE Anzeige, auch 30-100 Stueck. NICHT nur 2-3!

Gib NUR ein JSON zurueck (kein Markdown, keine ```json Marker):
{"anuncios": [{"ref": "...", "titulo": "...", "pueblo": "...", "comarca": "...", "precio": 90000, "precio_original": null, "m2_construida": null, "m2_parcela": 55000, "url": "https://...", "estado": "disponible", "tipo": "masia"}]}

Felder:
- ref: Referenznummer/ID als String
- titulo: Titel der Anzeige
- pueblo: Ortschaft
- comarca: Region (Baix Ebre, Priorat, Alt/Baix Camp, Alt/Baix Penedès, Ribera d'Ebre, Terra Alta, Tarragonès, Montsià)
- precio: Euro als Zahl (90000 - ohne Punkte/Kommas/Einheiten)
- precio_original: bei Rabatt, sonst null
- m2_construida: Haus-m² als Zahl, null wenn unbekannt
- m2_parcela: Grundstueck-m² als Zahl
- url: volle URL zur Anzeige
- estado: disponible | reservado | vendido | novedad | oportunidad | rebajado
- tipo: masia (mit Haus) | terreno (nur Grundstueck)

URL zum Analysieren: {url_target}
"""


def scrape_via_gemini(url, portal_name, retry=0):
    if not CONFIG["gemini_key"]:
        print("  ⚠️  GEMINI_API_KEY no configurada")
        return []

    # Gemini API Endpoint mit URL Context Tool
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={CONFIG['gemini_key']}"

    prompt = PROMPT_EXTRACCION.replace("{url_target}", url)

    body = {
        "contents": [{
            "parts": [{"text": prompt}]
        }],
        "tools": [{"url_context": {}}],
        "generationConfig": {
            "maxOutputTokens": 16000,
            "temperature": 0.1,
        }
    }

    try:
        r = requests.post(api_url, json=body, timeout=180)
        if r.status_code == 429 and retry < 3:
            print(f"      Rate limit, warte 30s... (retry {retry+1}/3)")
            time.sleep(30)
            return scrape_via_gemini(url, portal_name, retry + 1)
        if r.status_code == 503 and retry < 5:
            wait = 30 + (retry * 15)  # 30s, 45s, 60s, 75s, 90s
            print(f"      Gemini überlastet (503), warte {wait}s... (retry {retry+1}/5)")
            time.sleep(wait)
            return scrape_via_gemini(url, portal_name, retry + 1)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        print(f"      API error: {str(e)[:200]}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"      Response: {e.response.text[:300]}")
        return []

    # Text aus Antwort extrahieren
    try:
        candidates = data.get("candidates", [])
        if not candidates:
            print(f"      Keine Antwort von Gemini: {json.dumps(data)[:300]}")
            return []
        content = candidates[0].get("content", {})
        parts = content.get("parts", [])
        text_parts = [p.get("text", "") for p in parts if "text" in p]
        full_text = "\n".join(text_parts)
    except (KeyError, IndexError) as e:
        print(f"      Parse error: {e}")
        return []

    # Markdown-Marker entfernen
    full_text = re.sub(r'```(?:json)?\s*', '', full_text)
    full_text = re.sub(r'```', '', full_text)

    json_match = re.search(r'\{[\s\S]*"anuncios"[\s\S]*\}', full_text)
    if not json_match:
        print(f"      Kein JSON in Antwort")
        print(f"      Ausgabe: {full_text[:300]}")
        return []

    try:
        parsed = json.loads(json_match.group(0))
        anuncios = parsed.get("anuncios", [])
        for a in anuncios:
            a["fuente"] = portal_name
        return anuncios
    except json.JSONDecodeError as e:
        print(f"      JSON error: {e}")
        # Fallback: Control Characters entfernen und nochmal versuchen
        try:
            cleaned = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', '', json_match.group(0))
            parsed = json.loads(cleaned)
            anuncios = parsed.get("anuncios", [])
            for a in anuncios:
                a["fuente"] = portal_name
            print(f"      ✓ Gerettet: {len(anuncios)} Anzeigen nach Cleanup")
            return anuncios
        except json.JSONDecodeError:
            pass
        # Letzter Fallback: einzelne Objekte rausziehen
        try:
            objetos = re.findall(r'\{[^{}]*"ref"[^{}]*\}', json_match.group(0))
            anuncios = []
            for obj in objetos:
                try:
                    clean_obj = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', '', obj)
                    a = json.loads(clean_obj)
                    a["fuente"] = portal_name
                    anuncios.append(a)
                except:
                    continue
            if anuncios:
                print(f"      ✓ Gerettet: {len(anuncios)} Anzeigen einzeln")
                return anuncios
        except:
            pass
        return []


def recopilar_todos():
    todos = {}
    primer = True
    for portal in PORTALES:
        print(f"\n  {portal['nombre']}")
        for url in portal["urls"]:
            if not primer:
                time.sleep(DELAY_ENTRE_REQUESTS)
            primer = False
            print(f"    -> {url[:80]}")
            anuncios = scrape_via_gemini(url, portal["nombre"])
            print(f"       {len(anuncios)} anuncios")
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


def categorizar(a):
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
        if precio <= 150_000 and (ha is None or ha >= 1):
            return "perfecto", f"masia {precio}€ {ha}ha"
        if precio <= 200_000 and (ha is None or ha >= 1):
            return "bonus", f"masia {precio}€ {ha}ha (cerca)"
        if precio > 200_000:
            return "descartado", f"caro {precio}€"
        return "descartado", f"poca sup. {ha}ha"

    if tipo == "terreno":
        if precio <= 60_000 and (ha is None or ha >= 5):
            return "perfecto", f"terreno {precio}€ {ha}ha"
        if ha is not None and ha >= 20 and precio <= 150_000:
            return "bonus", f"terreno grande {ha}ha"
        if precio <= 90_000 and (ha is None or ha >= 3):
            return "bonus", f"terreno {precio}€ {ha}ha (cerca)"
        if precio > 150_000:
            return "descartado", f"caro {precio}€"
        return "descartado", f"poca sup."

    return "descartado", "?"


# ============ EMAIL ============

def fp(p):
    if p is None or p == 0: return "consultar"
    return f"{int(p):,} €".replace(",", ".")

def fh(m2):
    if not m2: return "—"
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
        return f'<span style="background:{bg}; color:white; font-size:10px; padding:3px 7px; border-radius:2px; font-weight:600; letter-spacing:0.5px; margin-left:8px;">{text}</span>'
    return ""


def render_tarjeta(a, es_bonus=False):
    tipo = clasificar(a)
    label = "MASIA" if tipo == "masia" else "TERRENO"
    color = "#8B4513" if tipo == "masia" else "#556B2F"

    cerca_badge = ""
    if es_bonus:
        cerca_badge = '<span style="background:#d97706; color:white; font-size:10px; padding:3px 7px; border-radius:2px; font-weight:600; letter-spacing:0.5px; margin-left:8px;">CERCA</span>'

    ubi = a.get("pueblo") or ""
    if a.get("comarca"):
        ubi += f" · {a['comarca']}"
    if not ubi: ubi = "Tarragona"

    precio_html = f'<div style="color:#1a1a1a; font-size:22px; font-weight:700; line-height:1;">{fp(a.get("precio"))}</div>'
    if a.get("precio_original") and a["precio_original"] != a.get("precio"):
        precio_html += f'<div style="color:#999; font-size:12px; text-decoration:line-through; margin-top:2px;">{fp(a["precio_original"])}</div>'

    ref = a.get("ref", "")
    url_prop = a.get("url") or "#"

    return f"""
    <tr><td style="padding:20px 24px; border-bottom:1px solid #eeeae2;">
      <div style="margin-bottom:12px;">
        <span style="background:{color}; color:white; font-size:10px; padding:3px 8px; border-radius:2px; font-weight:600; letter-spacing:1px;">{label}</span>
        {cerca_badge}
        {badge_estado(a.get("estado"))}
        <span style="color:#999; font-size:11px; margin-left:10px;">{a.get("fuente", "")}{" · " + str(ref) if ref else ""}</span>
      </div>
      <div style="margin-bottom:6px;">
        <a href="{url_prop}" style="color:#1a1a1a; font-size:17px; font-weight:600; text-decoration:none; line-height:1.3;">{a.get('titulo', 'Sin título')}</a>
      </div>
      <div style="color:#666; font-size:13px; margin-bottom:16px;">{ubi}</div>
      <table cellspacing="0" cellpadding="0" style="margin-bottom:14px;">
        <tr>
          <td style="padding-right:28px;">
            <div style="color:#999; font-size:10px; text-transform:uppercase; letter-spacing:1px; margin-bottom:4px;">Precio</div>
            {precio_html}
          </td>
          <td style="padding-right:28px;">
            <div style="color:#999; font-size:10px; text-transform:uppercase; letter-spacing:1px; margin-bottom:4px;">Parcela</div>
            <div style="color:#1a1a1a; font-size:16px; font-weight:600;">{fh(a.get("m2_parcela"))}</div>
            <div style="color:#888; font-size:11px;">{fm(a.get("m2_parcela"))}</div>
          </td>
          <td>
            <div style="color:#999; font-size:10px; text-transform:uppercase; letter-spacing:1px; margin-bottom:4px;">Construido</div>
            <div style="color:#1a1a1a; font-size:16px; font-weight:600;">{fm(a.get("m2_construida"))}</div>
          </td>
        </tr>
      </table>
      <a href="{url_prop}" style="background:#1a1a1a; color:white; padding:9px 18px; text-decoration:none; border-radius:2px; font-size:12px; font-weight:600; display:inline-block; letter-spacing:0.5px;">Ver propiedad</a>
    </td></tr>
    """


def construir_email(perfectos, bonus, total_scraped):
    # Monate auf Spanisch
    meses = ["", "enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"]
    d = datetime.now()
    fecha = f"{d.day} de {meses[d.month]} de {d.year}"

    p_masias   = sorted([a for a in perfectos if clasificar(a) == "masia"],   key=lambda a: a.get("precio") or 9e9)
    p_terrenos = sorted([a for a in perfectos if clasificar(a) == "terreno"], key=lambda a: a.get("precio") or 9e9)
    b_masias   = sorted([a for a in bonus     if clasificar(a) == "masia"],   key=lambda a: a.get("precio") or 9e9)
    b_terrenos = sorted([a for a in bonus     if clasificar(a) == "terreno"], key=lambda a: a.get("precio") or 9e9)

    bloques = ""

    if p_masias or p_terrenos:
        bloques += '<tr><td style="padding:24px 24px 6px; background:white; border-bottom:1px solid #eeeae2;"><div style="color:#999; font-size:10px; text-transform:uppercase; letter-spacing:2px; font-weight:600;">Cumplen criterios</div><h2 style="margin:4px 0 0; font-size:20px; color:#1a1a1a; font-weight:700;">Seleccion principal</h2></td></tr>'
        if p_masias:
            bloques += f'<tr><td style="padding:16px 24px 8px; background:#f8f5ef;"><div style="color:#666; font-size:11px; text-transform:uppercase; letter-spacing:1.5px; font-weight:600;">Masias · {len(p_masias)}</div></td></tr>'
            bloques += "".join(render_tarjeta(a) for a in p_masias)
        if p_terrenos:
            bloques += f'<tr><td style="padding:16px 24px 8px; background:#f8f5ef;"><div style="color:#666; font-size:11px; text-transform:uppercase; letter-spacing:1.5px; font-weight:600;">Terrenos · {len(p_terrenos)}</div></td></tr>'
            bloques += "".join(render_tarjeta(a) for a in p_terrenos)

    if b_masias or b_terrenos:
        bloques += '<tr><td style="padding:24px 24px 6px; background:white; border-top:1px solid #eeeae2; border-bottom:1px solid #eeeae2;"><div style="color:#d97706; font-size:10px; text-transform:uppercase; letter-spacing:2px; font-weight:600;">Cerca de los criterios</div><h2 style="margin:4px 0 0; font-size:20px; color:#1a1a1a; font-weight:700;">Tambien interesantes</h2></td></tr>'
        if b_masias:
            bloques += f'<tr><td style="padding:16px 24px 8px; background:#fdfaf3;"><div style="color:#666; font-size:11px; text-transform:uppercase; letter-spacing:1.5px; font-weight:600;">Masias · {len(b_masias)}</div></td></tr>'
            bloques += "".join(render_tarjeta(a, es_bonus=True) for a in b_masias)
        if b_terrenos:
            bloques += f'<tr><td style="padding:16px 24px 8px; background:#fdfaf3;"><div style="color:#666; font-size:11px; text-transform:uppercase; letter-spacing:1.5px; font-weight:600;">Terrenos · {len(b_terrenos)}</div></td></tr>'
            bloques += "".join(render_tarjeta(a, es_bonus=True) for a in b_terrenos)

    if not perfectos and not bonus:
        bloques = f'<tr><td style="padding:60px 24px; text-align:center; background:white;"><p style="margin:0 0 8px; font-size:15px; color:#1a1a1a;">Hoy no hay propiedades que cumplan los criterios.</p><p style="margin:0; font-size:13px; color:#999;">{total_scraped} propiedades analizadas.</p></td></tr>'

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0; padding:0; background:#efeae0; font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;">
<table width="100%" cellspacing="0" cellpadding="0" style="background:#efeae0; padding:24px 0;"><tr><td align="center">
<table width="640" cellspacing="0" cellpadding="0" style="background:white; border-radius:4px; overflow:hidden; box-shadow:0 2px 12px rgba(0,0,0,0.04);">

  <tr><td style="padding:40px 32px 32px; background:#1a1a1a;">
    <div style="color:#c74b2e; font-size:11px; letter-spacing:3px; font-weight:700; margin-bottom:8px;">CASA MUSA</div>
    <h1 style="margin:0; font-size:28px; color:white; font-weight:300; letter-spacing:-0.5px;">Informe diario de propiedades</h1>
    <div style="margin-top:16px; padding-top:16px; border-top:1px solid #333;">
      <span style="color:#999; font-size:13px;">{fecha}</span>
      <span style="color:#555; font-size:13px; margin:0 8px;">·</span>
      <span style="color:#ccc; font-size:13px;">{len(perfectos)} principales</span>
      <span style="color:#555; font-size:13px; margin:0 8px;">·</span>
      <span style="color:#ccc; font-size:13px;">{len(bonus)} interesantes</span>
    </div>
  </td></tr>

  {bloques}

  <tr><td style="padding:24px 32px; background:#f8f5ef; border-top:1px solid #eeeae2;">
    <div style="color:#666; font-size:12px; line-height:1.7;">
      <div style="color:#1a1a1a; font-weight:700; text-transform:uppercase; letter-spacing:1px; font-size:10px; margin-bottom:8px;">Criterios principales</div>
      Masias — hasta 150.000 €, mínimo 1 ha<br>
      Terrenos — hasta 60.000 €, mínimo 5 ha<br><br>
      <div style="color:#d97706; font-weight:700; text-transform:uppercase; letter-spacing:1px; font-size:10px; margin-bottom:8px;">Criterios cerca (interesantes)</div>
      Masias — hasta 200.000 €, mínimo 1 ha<br>
      Terrenos grandes — más de 20 ha, hasta 150.000 €
    </div>
  </td></tr>

  <tr><td style="padding:20px 32px; background:#1a1a1a; color:#666; font-size:11px; text-align:center; letter-spacing:1px;">
    CASA MUSA · Provincia de Tarragona · Alerta diaria
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
        asunto = f"Casa Musa · {nn} {'nueva' if nn == 1 else 'nuevas'} propiedades"
    else:
        asunto = f"Casa Musa · Informe del {datetime.now().strftime('%d/%m')}"

    msg["Subject"] = asunto
    msg["From"] = f"Casa Musa <{CONFIG['email_absender']}>"
    msg["To"] = ", ".join(dest)
    msg.attach(MIMEText(construir_email(perfectos, bonus, total_scraped), "html", "utf-8"))

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(CONFIG["smtp_server"], CONFIG["smtp_port"], timeout=30) as srv:
            srv.starttls(context=ctx)
            srv.login(CONFIG["email_absender"], CONFIG["email_passwort"])
            srv.sendmail(CONFIG["email_absender"], dest, msg.as_string())
        print(f"\n  Email a {len(dest)}: {np} principales + {nb} cerca ({nn} nuevas)")
        return True
    except Exception as e:
        print(f"  Error email: {e}")
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

    print(f"  Perfectos: {len(perfectos)}")
    print(f"  Bonus: {len(bonus)}")
    print(f"  Descartados: {len(descartados)}")

    if perfectos:
        print(f"\n  Perfectos:")
        for a in perfectos:
            print(f"    · {(a.get('titulo') or '')[:50]:50} | {a.get('precio')}€ | {m2_a_ha(a.get('m2_parcela'))}ha | {a.get('fuente','')}")

    if bonus:
        print(f"\n  Bonus:")
        for a in bonus:
            print(f"    · {(a.get('titulo') or '')[:50]:50} | {a.get('precio')}€ | {m2_a_ha(a.get('m2_parcela'))}ha | {a.get('fuente','')}")

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
