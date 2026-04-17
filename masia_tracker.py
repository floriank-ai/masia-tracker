"""
CASA MUSA - Masia Alert
Scraping via Claude API (web_fetch) - umgeht IP-Blockaden.
Multi-Portal: Buscomasia, Fotocasa (Via Augusta), Indomio, Milanuncios.
Kosten: ca. 1-2$/Monat bei taeglicher Ausfuehrung.
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

# ------------------------------------------------------------------ PORTALES
# Seiten die Claude fuer uns abruft. Claude kann oeffentliche Seiten
# auch scrapen wenn die Seite direkten Web-Scraper blockiert.

PORTALES = [
    {
        "nombre": "Buscomasia",
        "urls": [
            "https://www.buscomasia.com/venta/provincia-tarragona/precio-asc/",
            "https://www.buscomasia.com/priorat/",
        ],
    },
    {
        "nombre": "Fotocasa (Finques Via Augusta)",
        "urls": [
            "https://www.fotocasa.es/es/inmobiliaria-finques-via-augusta/comprar/inmuebles/espana/todas-las-zonas/l?clientId=9202754898213",
        ],
    },
    {
        "nombre": "Indomio (Finques Via Augusta)",
        "urls": [
            "https://www.indomio.es/agencias-inmobiliarias/381668/finques-via-augusta/",
        ],
    },
    {
        "nombre": "Milanuncios Tarragona",
        "urls": [
            "https://www.milanuncios.com/fincas-rusticas-en-tarragona/",
        ],
    },
]


PROMPT_EXTRACCION = """Analisiere das folgende HTML einer spanischen Immobilien-Webseite.
Extrahiere ALLE Immobilien-Anzeigen (Masias, Fincas rusticas, Terrenos, Casas de campo).

Ignoriere: Pisos in Staedten, Wohnungen, Garagen, Buerogebaeude.

Gib als JSON zurueck, Format: {"anuncios": [...]}

Fuer jede Anzeige:
- ref: Referenznummer oder ID (String, wenn nicht vorhanden: generiere aus URL)
- titulo: vollstaendiger Titel
- pueblo: Ort/Gemeinde
- comarca: Region (Baix Ebre, Priorat, Alt Camp, Baix Penedès, Ribera d'Ebre, Terra Alta, etc.)
- precio: aktueller Preis in Euro als Ganzzahl (ohne Punkte/Kommas, z.B. 90000)
- precio_original: falls rabattiert, sonst null
- m2_construida: Haus-Flaeche in m2 als Zahl, null wenn nicht vorhanden
- m2_parcela: Grundstuecks-Flaeche in m2 als Zahl, null wenn nicht vorhanden
- url: vollstaendige URL
- estado: "disponible" | "reservado" | "vendido" | "novedad" | "oportunidad" | "rebajado"
- tipo: "masia" (mit Gebaeude) | "terreno" (nur Grundstueck)

WICHTIG:
- Gib NUR valides JSON zurueck, KEIN Markdown, KEINE Erklaerungen
- Wenn keine passenden Anzeigen: {"anuncios": []}
- Preis als Ganzzahl ohne Tausendertrennung
"""


# ------------------------------------------------------------------ API CALL

def scrape_via_claude(url, portal_name):
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
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 8000,
        "tools": [{
            "type": "web_fetch_20250910",
            "name": "web_fetch",
            "max_uses": 2,
        }],
        "messages": [{
            "role": "user",
            "content": f"{PROMPT_EXTRACCION}\n\nRufe diese URL ab und extrahiere alle passenden Anzeigen: {url}"
        }]
    }

    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
                          headers=headers, json=body, timeout=180)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        print(f"  Error API: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"  Response: {e.response.text[:300]}")
        return []

    text_parts = []
    for block in data.get("content", []):
        if block.get("type") == "text":
            text_parts.append(block.get("text", ""))
    full_text = "\n".join(text_parts)

    json_match = re.search(r'\{[\s\S]*"anuncios"[\s\S]*\}', full_text)
    if not json_match:
        print(f"  Kein JSON in Antwort")
        return []

    try:
        parsed = json.loads(json_match.group(0))
        anuncios = parsed.get("anuncios", [])
        for a in anuncios:
            a["fuente"] = portal_name
        return anuncios
    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        return []


def recopilar_todos():
    todos = {}
    for portal in PORTALES:
        print(f"\n  📡 {portal['nombre']}")
        for url in portal["urls"]:
            print(f"    {url[:70]}...")
            anuncios = scrape_via_claude(url, portal["nombre"])
            print(f"      -> {len(anuncios)} anuncios")
            for a in anuncios:
                ref = str(a.get("ref", "")).strip() or a.get("url", "")[-30:]
                a_id = f"{portal['nombre']}_{ref}".replace(" ", "_")
                if a_id not in todos:
                    a["id"] = a_id
                    todos[a_id] = a
            time.sleep(1)
    return list(todos.values())


# ------------------------------------------------------------------ HELPERS

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


# ------------------------------------------------------------------ FILTER

REGIONES_INTERES = [
    "tarragona", "priorat", "baix ebre", "terra alta", "ribera d'ebre", "ribera de ebro",
    "montsia", "montsià", "alt penedes", "alt penedès", "baix penedes", "baix penedès",
    "alt camp", "baix camp", "conca de barbera", "conca de barberà",
    "tarragones", "tarragonès", "perelló", "perello", "tortosa", "rasquera",
    "ametlla", "cambrils", "reus", "falset", "montblanc", "vendrell"
]


def clasificar(a):
    tipo_explicito = (a.get("tipo") or "").lower()
    if tipo_explicito in ("masia", "terreno"):
        return tipo_explicito

    t = (a.get("titulo") or "").lower()
    edificio = any(p in t for p in ["masia", "masía", "casa", "finca", "vivienda",
                                     "hotel", "mas ", "ruina", "xalet", "chalet",
                                     "hectáreas con" , "con vivienda"])
    solo_terreno = any(p in t for p in ["terreno", "parcela", "solar", "ackerland"]) and not edificio
    if solo_terreno:
        return "terreno"
    return "masia" if edificio else "terreno"


def cumple_criterios(a):
    estado = (a.get("estado") or "").lower()
    if estado == "vendido":
        return False

    precio = a.get("precio")
    if precio is None or precio <= 0:
        return False

    # Region check
    region_texto = f"{a.get('pueblo', '')} {a.get('comarca', '')}".lower()
    if not any(r in region_texto for r in REGIONES_INTERES):
        # Toleranter sein - wenn keine comarca-info vorhanden, durchlassen
        if a.get("comarca"):
            return False

    tipo = clasificar(a)
    m2p = a.get("m2_parcela")
    ha = m2_a_ha(m2p) if m2p else None

    if tipo == "masia":
        if precio > 150_000:
            return False
        if ha is not None and ha < 5:
            return False
        return True

    if tipo == "terreno":
        if precio > 60_000:
            return False
        if ha is not None and ha < 10:
            return False
        return True

    return False


# ------------------------------------------------------------------ EMAIL

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


def render_tarjeta(a):
    tipo = clasificar(a)
    label = "🏠 MASIA" if tipo == "masia" else "🌾 TERRENO"
    color = "#8B4513" if tipo == "masia" else "#556B2F"
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


def construir_email(calificados, nuevos):
    fecha = datetime.now().strftime("%d.%m.%Y")
    masias = sorted([a for a in calificados if clasificar(a) == "masia"],
                    key=lambda a: a.get("precio") or 9e9)
    terrenos = sorted([a for a in calificados if clasificar(a) == "terreno"],
                      key=lambda a: a.get("precio") or 9e9)

    # Stats pro Portal
    stats_portales = {}
    for a in calificados:
        f = a.get("fuente", "Unknown")
        stats_portales[f] = stats_portales.get(f, 0) + 1
    stats_html = " · ".join(f"{n} {k.split('(')[0].strip()}" for k, n in stats_portales.items())

    bl_masias = ""
    if masias:
        header = f'<tr><td style="padding:20px 24px 10px; background:#f5f1ea;"><h2 style="margin:0; font-size:17px; color:#1a1a1a;">🏠 Masias ({len(masias)})</h2></td></tr>'
        bl_masias = header + "".join(render_tarjeta(a) for a in masias)

    bl_terrenos = ""
    if terrenos:
        header = f'<tr><td style="padding:20px 24px 10px; background:#f5f1ea;"><h2 style="margin:0; font-size:17px; color:#1a1a1a;">🌾 Terrenos ({len(terrenos)})</h2></td></tr>'
        bl_terrenos = header + "".join(render_tarjeta(a) for a in terrenos)

    bl_vacio = ""
    if not calificados:
        bl_vacio = '<tr><td style="padding:40px 24px; text-align:center; color:#666;"><p style="margin:0; font-size:15px;">Hoy no hay propiedades que cumplan los criterios.</p></td></tr>'

    n = len(nuevos)
    total = len(calificados)

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0; padding:0; background:#efeae0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<table width="100%" cellspacing="0" cellpadding="0" style="background:#efeae0; padding:20px 0;"><tr><td align="center">
<table width="640" cellspacing="0" cellpadding="0" style="background:white; border-radius:6px; overflow:hidden;">

  <tr><td style="padding:28px 24px; background:#1a1a1a; color:white;">
    <div style="font-size:12px; letter-spacing:2px; color:#c74b2e; font-weight:600;">CASA MUSA</div>
    <h1 style="margin:6px 0 0; font-size:24px; font-weight:700;">Masia Alert</h1>
    <p style="margin:8px 0 0; font-size:13px; color:#bbb;">{fecha} · {total} propiedades · {n} {'nueva' if n == 1 else 'nuevas'}</p>
    {f'<p style="margin:4px 0 0; font-size:11px; color:#888;">{stats_html}</p>' if stats_html else ''}
  </td></tr>

  {bl_vacio}{bl_masias}{bl_terrenos}

  <tr><td style="padding:20px 24px; background:#fafaf7; border-top:1px solid #e5e5e5;">
    <div style="color:#666; font-size:12px; line-height:1.6;">
      <strong style="color:#1a1a1a;">Criterios:</strong><br>
      🏠 Masias: 5-10 ha · max 150.000 €<br>
      🌾 Terrenos: min 10 ha · max 60.000 €<br>
      📍 Provincia Tarragona (Priorat, Penedès, Baix Ebre, Terra Alta, Ribera d'Ebre)
    </div>
  </td></tr>

  <tr><td style="padding:16px 24px; background:#1a1a1a; color:#888; font-size:11px; text-align:center;">
    CASA MUSA · Alerta diaria 08:00
  </td></tr>

</table></td></tr></table></body></html>"""


def enviar_email(calificados, nuevos):
    if not CONFIG["email_absender"] or not CONFIG["email_passwort"]:
        print("  ⚠️  Credenciales email no configuradas")
        return False

    dest = [e.strip() for e in CONFIG["email_empfaenger"].split(",") if e.strip()]
    if not dest:
        return False

    msg = MIMEMultipart("alternative")
    n = len(nuevos)
    total = len(calificados)
    if n > 0:
        asunto = f"CASA MUSA · {n} {'nueva' if n == 1 else 'nuevas'} · {total} total"
    else:
        asunto = f"CASA MUSA · {total} propiedades · {datetime.now().strftime('%d.%m')}"

    msg["Subject"] = asunto
    msg["From"] = CONFIG["email_absender"]
    msg["To"] = ", ".join(dest)
    msg.attach(MIMEText(construir_email(calificados, nuevos), "html", "utf-8"))

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(CONFIG["smtp_server"], CONFIG["smtp_port"], timeout=30) as srv:
            srv.starttls(context=ctx)
            srv.login(CONFIG["email_absender"], CONFIG["email_passwort"])
            srv.sendmail(CONFIG["email_absender"], dest, msg.as_string())
        print(f"\n  ✉️  Email a {len(dest)}: {total} total, {n} nuevas")
        return True
    except Exception as e:
        print(f"  ❌ Error email: {e}")
        return False


# ------------------------------------------------------------------ MAIN

def ejecutar():
    print("=" * 60)
    print(f"  CASA MUSA · {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 60)

    vistos = cargar_vistos()
    print(f"  Ya vistos: {len(vistos)}")

    todos = recopilar_todos()
    print(f"\n  Total scraped: {len(todos)}")

    calificados = [a for a in todos if cumple_criterios(a)]
    print(f"  Califican: {len(calificados)}")

    nuevos = [a for a in calificados if a["id"] not in vistos]
    print(f"  Nuevas: {len(nuevos)}")

    enviar_email(calificados, nuevos)

    for a in calificados:
        vistos.add(a["id"])
    guardar_vistos(vistos)
    print(f"  Historial: {len(vistos)}")
    print("=" * 60)


if __name__ == "__main__":
    ejecutar()
