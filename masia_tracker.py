"""
CASA MUSA - Masia Alert
Fuente principal: Buscomasia (listado directo con precio, superficie, ubicacion)
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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ------------------------------------------------------------------ CONFIG
CONFIG = {
    "email_absender":   os.environ.get("EMAIL_ABSENDER",   ""),
    "email_passwort":   os.environ.get("EMAIL_PASSWORT",   ""),
    "email_empfaenger": os.environ.get("EMAIL_EMPFAENGER", ""),
    "smtp_server":      "smtp.gmail.com",
    "smtp_port":        587,

    # Masias
    "masia_precio_max":    150_000,
    "masia_superficie_min_ha": 5,
    "masia_superficie_max_ha": 10,

    # Terrenos (solo con ruina o casa/masia)
    "terreno_precio_max":   60_000,
    "terreno_superficie_min_ha": 10,

    # Regiones de interes (para filtrado / score)
    "regiones_interes": [
        "tarragona", "priorat", "baix ebre", "terra alta",
        "ribera d'ebre", "ribera de ebro", "montsia", "montsià",
        "alt penedes", "alt penedès", "baix penedes", "baix penedès",
        "alt camp", "baix camp", "conca de barbera", "conca de barberà",
        "tarragones", "tarragonès"
    ],

    "archivo_vistos": "masia_gesehen.json",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,ca;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
}


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


def parse_numero(texto):
    """'1.300.000 €' -> 1300000, '520.000 m2' -> 520000, '14.000' -> 14000"""
    if not texto:
        return None
    # quitar todo menos digitos, punto, coma
    limpio = re.sub(r"[^\d.,]", "", texto)
    if not limpio:
        return None
    # formato europeo: . = miles, , = decimal
    limpio = limpio.replace(".", "").replace(",", ".")
    try:
        return float(limpio)
    except ValueError:
        return None


def m2_a_ha(m2):
    if m2 is None:
        return None
    return round(m2 / 10_000, 2)


# ------------------------------------------------------------------ SCRAPING

URLS_BUSCOMASIA = [
    # Provincia y comarcas relevantes
    "https://www.buscomasia.com/venta/provincia-tarragona/",
    "https://www.buscomasia.com/priorat/",
    "https://www.buscomasia.com/alt-penedes/",
    "https://www.buscomasia.com/baix-camp/",
    "https://www.buscomasia.com/alt-camp/",
    "https://www.buscomasia.com/tarragona/",
]


def scrape_buscomasia_listado(url):
    """
    Parsea el listado directamente - cada propiedad es una tarjeta con:
    - enlace /venta/XXX-REF/
    - titulo
    - ubicacion "Alcover - Alt Camp"
    - Ref. NNNN
    - m2 construidos + m2 parcela + habitaciones + baños
    - precio
    """
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error {url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    anuncios = []

    # Buscar todos los enlaces a propiedades individuales
    # Formato: /venta/slug-REFNUM/
    enlaces_propiedades = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"/venta/([a-z0-9-]+-(\d+))/?$", href)
        if m:
            url_abs = urljoin("https://www.buscomasia.com", href)
            enlaces_propiedades.add((url_abs, m.group(2)))

    # Para cada propiedad, buscar su bloque de informacion en la pagina
    # Los datos aparecen despues del ultimo enlace de galeria de cada propiedad
    texto_completo = soup.get_text("\n", strip=True)

    # Estrategia alternativa: parsear por bloques de referencia
    # Cada tarjeta contiene "Ref. NNNN" seguido del titulo, ubicacion y datos
    # Extraemos todos los bloques con regex sobre el texto

    for url_prop, ref in enlaces_propiedades:
        # Buscar el bloque de esta propiedad en el texto
        # Patron: "[PRECIO] €\n\n[TITULO]\n\n[UBICACION]\n\nRef. NNNN\n\n... [m2 construida] m\n2\n...[m2 parcela] m\n2"
        bloque_regex = rf"Ref\.\s*{ref}\b"
        m = re.search(bloque_regex, texto_completo)
        if not m:
            continue

        # Tomar contexto antes (titulo, ubicacion, precio) y despues (m2)
        inicio = max(0, m.start() - 500)
        fin = min(len(texto_completo), m.end() + 500)
        contexto = texto_completo[inicio:fin]

        # Precio: numero seguido de €, antes del titulo
        precio = None
        precios_encontrados = re.findall(r"([\d.,]+)\s*€", contexto[:500])
        if precios_encontrados:
            # Puede haber "1.300.000 €" y "1.600.000 €" (descuento) - tomar el ultimo (precio actual)
            precio_raw = precios_encontrados[-1]
            precio = parse_numero(precio_raw)

        # Vendido?
        vendido = "Vendido" in contexto[:500]

        # Titulo: linea antes de la ubicacion
        # Ubicacion: "Pueblo - Comarca"
        ubicacion_match = re.search(r"([A-ZÀ-ÿ][A-Za-zÀ-ÿ' ]+)\s*-\s*([A-ZÀ-ÿ][A-Za-zÀ-ÿ' ]+)\s*\nRef\.", contexto)
        pueblo = ubicacion_match.group(1).strip() if ubicacion_match else ""
        comarca = ubicacion_match.group(2).strip() if ubicacion_match else ""

        # Titulo: antes de la ubicacion
        titulo = ""
        if ubicacion_match:
            antes = contexto[:ubicacion_match.start()].strip()
            # ultima linea significativa
            lineas = [l.strip() for l in antes.split("\n") if l.strip() and "€" not in l and "Vendido" not in l]
            if lineas:
                titulo = lineas[-1]

        # m2: buscar despues de "Ref."
        despues = contexto[m.end():]
        # patron "NUMERO m\n2" - primero construida, luego parcela
        m2_matches = re.findall(r"([\d.,]+)\s*m\s*\n?\s*2", despues)
        m2_construida = parse_numero(m2_matches[0]) if len(m2_matches) >= 1 else None
        m2_parcela    = parse_numero(m2_matches[1]) if len(m2_matches) >= 2 else None

        # Habitaciones y baños
        hab_match = re.search(r"\n(\d+(?:\s*\+\s*\d+)?)\s*\n", despues)

        anuncios.append({
            "id": f"buscomasia_{ref}",
            "ref": ref,
            "url": url_prop,
            "titulo": titulo or f"Propiedad {ref}",
            "pueblo": pueblo,
            "comarca": comarca,
            "precio": precio,
            "m2_construida": m2_construida,
            "m2_parcela": m2_parcela,
            "ha_parcela": m2_a_ha(m2_parcela),
            "vendido": vendido,
            "fuente": "Buscomasia",
        })

    return anuncios


def recopilar_todos():
    todos = {}  # id -> anuncio (deduplicar)
    for url in URLS_BUSCOMASIA:
        print(f"  Scraping {url}")
        resultados = scrape_buscomasia_listado(url)
        print(f"    -> {len(resultados)} anuncios")
        for a in resultados:
            if a["id"] not in todos:
                todos[a["id"]] = a
        time.sleep(2)  # ser respetuoso
    return list(todos.values())


# ------------------------------------------------------------------ FILTRADO

def clasificar(a):
    """Devuelve 'masia', 'terreno' o None si no aplica."""
    titulo_lower = a["titulo"].lower()
    tiene_edificio = any(p in titulo_lower for p in [
        "masia", "masía", "casa", "finca", "vivienda", "hotel",
        "mas ", "ruina", "ruína", "edifici", "cortijo"
    ])
    es_solo_terreno = any(p in titulo_lower for p in [
        "terreno", "parcela", "solar", "suelo"
    ]) and not tiene_edificio

    if es_solo_terreno:
        return "terreno"
    if tiene_edificio:
        return "masia"
    # por defecto, si esta en buscomasia y no menciona terreno -> masia
    return "masia"


def cumple_criterios(a):
    """Filtra segun precio, superficie, region."""
    if a["vendido"]:
        return False, "vendido"
    if a["precio"] is None:
        return False, "sin precio"

    tipo = clasificar(a)

    # Region check
    region_texto = f"{a['pueblo']} {a['comarca']}".lower()
    region_ok = any(r in region_texto for r in CONFIG["regiones_interes"])
    if not region_ok and a["comarca"]:  # si hay comarca pero no coincide
        return False, f"region fuera: {a['comarca']}"

    if tipo == "masia":
        if a["precio"] > CONFIG["masia_precio_max"]:
            return False, f"precio masia > {CONFIG['masia_precio_max']:,}€"
        if a["ha_parcela"] is not None:
            if a["ha_parcela"] < CONFIG["masia_superficie_min_ha"]:
                return False, f"superficie masia < {CONFIG['masia_superficie_min_ha']} ha"
            if a["ha_parcela"] > CONFIG["masia_superficie_max_ha"] * 10:
                # permitir hasta 10x el rango (flexibilidad para fincas grandes)
                # si quieres estricto 5-10ha, cambia a: > CONFIG['masia_superficie_max_ha']
                pass
        return True, "masia ok"

    if tipo == "terreno":
        if a["precio"] > CONFIG["terreno_precio_max"]:
            return False, f"precio terreno > {CONFIG['terreno_precio_max']:,}€"
        if a["ha_parcela"] is not None and a["ha_parcela"] < CONFIG["terreno_superficie_min_ha"]:
            return False, f"superficie terreno < {CONFIG['terreno_superficie_min_ha']} ha"
        return True, "terreno ok"

    return False, "sin clasificar"


# ------------------------------------------------------------------ EMAIL

LINKS_PORTALES = [
    ("Idealista - Tarragona fincas",
     "https://www.idealista.com/venta-viviendas/tarragona-provincia/?tipo=finca&precioMax=150000"),
    ("Idealista - Priorat",
     "https://www.idealista.com/venta-viviendas/priorat-tarragona/?tipo=finca&precioMax=150000"),
    ("Idealista - Terres de l'Ebre",
     "https://www.idealista.com/venta-viviendas/terres-de-l-ebre-tarragona/?tipo=finca&precioMax=150000"),
    ("Fotocasa - Casas de campo Tarragona",
     "https://www.fotocasa.es/es/comprar/casas/tarragona-provincia/todas-las-zonas/l"),
    ("Kyero - Fincas Tarragona",
     "https://www.kyero.com/es/propiedad-en-venta/tarragona?precio_max=150000"),
    ("Habitaclia - Masias Tarragona",
     "https://www.habitaclia.com/comprar-casa_rural-tarragona_provincia.htm"),
    ("Milanuncios - Fincas Tarragona",
     "https://www.milanuncios.com/fincas-rusticas/tarragona.htm"),
    ("Buscomasia - Tarragona",
     "https://www.buscomasia.com/venta/provincia-tarragona/"),
    ("Finques Via Augusta",
     "https://www.finquesviaaugusta.com/es/propiedades"),
    ("Terrenos.es - Tarragona",
     "https://tarragona.terrenos.es/"),
]


def formato_precio(p):
    if p is None:
        return "consultar"
    return f"{int(p):,} €".replace(",", ".")


def formato_ha(ha):
    if ha is None:
        return "? ha"
    return f"{ha:.1f} ha".replace(".", ",")


def formato_m2(m2):
    if m2 is None:
        return "? m²"
    return f"{int(m2):,} m²".replace(",", ".")


def render_tarjeta(a):
    tipo = clasificar(a)
    tipo_label = "🏠 MASIA" if tipo == "masia" else "🌾 TERRENO"
    badge_color = "#8B4513" if tipo == "masia" else "#556B2F"

    ubicacion = a["pueblo"]
    if a["comarca"]:
        ubicacion += f" · {a['comarca']}"
    if not ubicacion:
        ubicacion = "Tarragona"

    return f"""
    <tr>
      <td style="padding:18px; border-bottom:1px solid #e5e5e5;">
        <table width="100%" cellspacing="0" cellpadding="0">
          <tr>
            <td>
              <span style="background:{badge_color}; color:white; font-size:11px; padding:3px 8px; border-radius:3px; font-weight:600; letter-spacing:0.5px;">{tipo_label}</span>
              <span style="color:#888; font-size:11px; margin-left:8px;">Ref. {a['ref']} · {a['fuente']}</span>
            </td>
          </tr>
          <tr>
            <td style="padding-top:10px;">
              <a href="{a['url']}" style="color:#1a1a1a; font-size:17px; font-weight:600; text-decoration:none; line-height:1.3;">{a['titulo']}</a>
            </td>
          </tr>
          <tr>
            <td style="padding-top:6px; color:#666; font-size:13px;">
              📍 {ubicacion}
            </td>
          </tr>
          <tr>
            <td style="padding-top:12px;">
              <table cellspacing="0" cellpadding="0">
                <tr>
                  <td style="padding-right:20px;">
                    <div style="color:#999; font-size:11px; text-transform:uppercase; letter-spacing:0.5px;">Precio</div>
                    <div style="color:#c74b2e; font-size:20px; font-weight:700;">{formato_precio(a['precio'])}</div>
                  </td>
                  <td style="padding-right:20px;">
                    <div style="color:#999; font-size:11px; text-transform:uppercase; letter-spacing:0.5px;">Parcela</div>
                    <div style="color:#1a1a1a; font-size:16px; font-weight:600;">{formato_ha(a['ha_parcela'])}</div>
                    <div style="color:#888; font-size:11px;">{formato_m2(a['m2_parcela'])}</div>
                  </td>
                  <td>
                    <div style="color:#999; font-size:11px; text-transform:uppercase; letter-spacing:0.5px;">Construido</div>
                    <div style="color:#1a1a1a; font-size:16px; font-weight:600;">{formato_m2(a['m2_construida'])}</div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          <tr>
            <td style="padding-top:14px;">
              <a href="{a['url']}" style="background:#1a1a1a; color:white; padding:9px 18px; text-decoration:none; border-radius:4px; font-size:13px; font-weight:600; display:inline-block;">Ver propiedad →</a>
            </td>
          </tr>
        </table>
      </td>
    </tr>
    """


def render_botones_portales():
    botones = ""
    for nombre, url in LINKS_PORTALES:
        botones += f"""
        <a href="{url}" style="display:inline-block; background:#f5f1ea; color:#1a1a1a; padding:10px 16px; text-decoration:none; border-radius:4px; font-size:13px; margin:4px 4px 4px 0; border:1px solid #e0dcd3;">{nombre} →</a>
        """
    return botones


def construir_email(anuncios_nuevos, anuncios_totales):
    fecha = datetime.now().strftime("%d.%m.%Y")
    n_nuevos = len(anuncios_nuevos)

    # Separar por tipo
    masias = [a for a in anuncios_nuevos if clasificar(a) == "masia"]
    terrenos = [a for a in anuncios_nuevos if clasificar(a) == "terreno"]

    # Ordenar por precio ascendente
    masias.sort(key=lambda a: a["precio"] or 999999999)
    terrenos.sort(key=lambda a: a["precio"] or 999999999)

    bloque_masias = ""
    if masias:
        bloque_masias = f"""
        <tr><td style="padding:20px 20px 10px; background:#f5f1ea;">
          <h2 style="margin:0; font-size:18px; color:#1a1a1a;">🏠 Masias ({len(masias)})</h2>
        </td></tr>
        {''.join(render_tarjeta(a) for a in masias)}
        """

    bloque_terrenos = ""
    if terrenos:
        bloque_terrenos = f"""
        <tr><td style="padding:20px 20px 10px; background:#f5f1ea;">
          <h2 style="margin:0; font-size:18px; color:#1a1a1a;">🌾 Terrenos ({len(terrenos)})</h2>
        </td></tr>
        {''.join(render_tarjeta(a) for a in terrenos)}
        """

    if not masias and not terrenos:
        bloque_sin_nuevos = """
        <tr><td style="padding:30px; text-align:center; color:#666; background:#fafaf7;">
          <p style="margin:0; font-size:15px;">Hoy no hay propiedades nuevas que cumplan los criterios.</p>
          <p style="margin:8px 0 0; font-size:13px; color:#888;">Revisa los portales directamente abajo.</p>
        </td></tr>
        """
    else:
        bloque_sin_nuevos = ""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0; padding:0; background:#efeae0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <table width="100%" cellspacing="0" cellpadding="0" style="background:#efeae0; padding:20px 0;">
    <tr><td align="center">
      <table width="620" cellspacing="0" cellpadding="0" style="background:white; border-radius:6px; overflow:hidden;">

        <tr><td style="padding:28px 24px; background:#1a1a1a; color:white;">
          <div style="font-size:12px; letter-spacing:2px; color:#c74b2e; font-weight:600;">CASA MUSA</div>
          <h1 style="margin:6px 0 0; font-size:24px; font-weight:700;">Masia Alert</h1>
          <p style="margin:8px 0 0; font-size:13px; color:#bbb;">{fecha} · {n_nuevos} {'propiedad nueva' if n_nuevos == 1 else 'propiedades nuevas'} · {anuncios_totales} en seguimiento</p>
        </td></tr>

        {bloque_sin_nuevos}
        {bloque_masias}
        {bloque_terrenos}

        <tr><td style="padding:24px; background:#fafaf7; border-top:2px solid #e5e5e5;">
          <h3 style="margin:0 0 12px; font-size:14px; color:#1a1a1a; text-transform:uppercase; letter-spacing:1px;">🔗 Buscar en portales</h3>
          <p style="margin:0 0 14px; font-size:12px; color:#666;">Idealista, Fotocasa y Kyero bloquean scraping automatico. Haz clic para ver resultados actualizados:</p>
          <div>{render_botones_portales()}</div>
        </td></tr>

        <tr><td style="padding:20px 24px; background:#1a1a1a; color:#888; font-size:11px; text-align:center;">
          Criterios: Masias 5-10ha max 150k€ · Terrenos (con ruina/casa) min 10ha max 60k€<br>
          Regiones: Tarragona, Priorat, Penedès, Baix Ebre, Terra Alta, Ribera d'Ebre<br>
          <span style="color:#555;">Alerta diaria 08:00</span>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return html


def enviar_email(anuncios_nuevos, anuncios_totales):
    if not CONFIG["email_absender"] or not CONFIG["email_passwort"]:
        print("  ⚠️  Credenciales de email no configuradas")
        return False

    destinatarios = [e.strip() for e in CONFIG["email_empfaenger"].split(",") if e.strip()]
    if not destinatarios:
        print("  ⚠️  No hay destinatarios")
        return False

    msg = MIMEMultipart("alternative")
    n = len(anuncios_nuevos)
    if n == 0:
        asunto = f"CASA MUSA Masia Alert · {datetime.now().strftime('%d.%m')} · sin novedades"
    else:
        asunto = f"CASA MUSA Masia Alert · {n} {'nueva' if n == 1 else 'nuevas'}"

    msg["Subject"] = asunto
    msg["From"] = CONFIG["email_absender"]
    msg["To"] = ", ".join(destinatarios)

    html = construir_email(anuncios_nuevos, anuncios_totales)
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(CONFIG["smtp_server"], CONFIG["smtp_port"], timeout=30) as srv:
            srv.starttls(context=ctx)
            srv.login(CONFIG["email_absender"], CONFIG["email_passwort"])
            srv.sendmail(CONFIG["email_absender"], destinatarios, msg.as_string())
        print(f"  ✉️  Email enviado a {len(destinatarios)} destinatarios: {n} anuncios")
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

    # Filtrar segun criterios
    calificados = []
    descartados = 0
    for a in todos:
        cumple, razon = cumple_criterios(a)
        if cumple:
            calificados.append(a)
        else:
            descartados += 1
    print(f"  Califican: {len(calificados)} · Descartados: {descartados}")

    # Separar nuevos
    nuevos = [a for a in calificados if a["id"] not in vistos]
    print(f"  Nuevos (no vistos antes): {len(nuevos)}")

    # Enviar siempre
    enviar_email(nuevos, len(calificados))

    # Marcar como vistos (solo los nuevos que pasaron filtro)
    for a in nuevos:
        vistos.add(a["id"])
    guardar_vistos(vistos)

    print(f"\n  Guardados {len(vistos)} anuncios en historial")
    print("=" * 60)


if __name__ == "__main__":
    ejecutar()
