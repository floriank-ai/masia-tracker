"""
CASA MUSA — Masia Alert
Fuentes: Buscomasia (detalle), Finques Via Augusta, Idealista, Kyero, Fotocasa
"""

import json, os, smtplib, hashlib, re, time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
from bs4 import BeautifulSoup

# ============================================================
#  CONFIGURACION
# ============================================================
CONFIG = {
    "email_absender":   os.getenv("EMAIL_ABSENDER",   "tu@gmail.com"),
    "email_passwort":   os.getenv("EMAIL_PASSWORT",   "APP_PASSWORD"),
    "email_empfaenger": os.getenv("EMAIL_EMPFAENGER", "destino@ejemplo.com"),

    "masia_precio_max":   150_000,
    "masia_ha_min":       5,
    "masia_ha_max":       10,
    "terreno_precio_max": 60_000,
    "terreno_ha_min":     10,

    "regiones_buscomasia": [
        "venta/provincia-tarragona",
        "priorat",
        "alt-penedes",
        "baix-camp",
    ],
    "palabras_edificio": [
        "masia", "masoveria", "casa", "cabana", "ruina", "ruines",
        "edificio", "habitable", "construccion", "finca con casa",
        "pages", "mas ", "maso", "habitada", "vivienda",
    ],
    "palabras_pueblo": [
        "casco urbano", "centro urbano", "calle ", "carrer ",
        "plaza ", "placa ", "barrio", "piso", "apartamento",
        "passeig", "paseo ", "al centre del", "en el pueblo",
    ],
    "palabras_aislada": [
        "aislada", "aislado", "finca aislada", "entorno rural",
        "campo", "zona rural", "finca rustica", "rustico", "rustica",
        "mas ", "maso", "fuera del pueblo", "naturaleza",
    ],
}

SEEN_FILE = "masia_gesehen.json"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
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

# ============================================================
#  HELPERS
# ============================================================
def extraer_precio(t):
    if not t: return None
    for z in re.findall(r"[\d\.]+", str(t).replace(",",".")):
        try:
            v = int(float(z.replace(".","")))
            if 1000 < v < 10_000_000: return v
        except: pass
    return None

def extraer_ha(t):
    if not t: return None
    tl = str(t).lower()
    # Formato "44,35 hectáreas" o "44.35 ha"
    m = re.search(r"([\d\.,]+)\s*(hectar|ha\b)", tl)
    if m:
        try: return float(m.group(1).replace(",","."))
        except: pass
    # Formato m2 con puntos de miles: "443.500 m2"
    m = re.search(r"([\d\.,]+)\s*m[\s2²]", tl)
    if m:
        try:
            v = float(m.group(1).replace(".","").replace(",","."))
            if v > 1000: return round(v/10000, 2)
        except: pass
    return None

def tiene_agua(t):
    return any(k in str(t).lower() for k in [
        "agua","pozo","fuente","manantial","suministro agua",
        "red de agua","agua potable","agua corriente","agua municipal","pou"])

def tiene_luz(t):
    return any(k in str(t).lower() for k in [
        "luz","electricidad","electrica","electrico","corriente electrica",
        "suministro electrico","red electrica","placas solares","solar",
        "fotovoltaica","autoconsumo","luz y agua","agua y luz"])

def tiene_edificio(t):
    return any(k in str(t).lower() for k in CONFIG["palabras_edificio"])

def en_pueblo(t):
    return any(k in str(t).lower() for k in CONFIG["palabras_pueblo"])

def es_aislada(t):
    return any(k in str(t).lower() for k in CONFIG["palabras_aislada"])

def tiene_mar(t):
    return any(k in str(t).lower() for k in [
        "vista al mar","vistas al mar","mar ","costa","mediterraneo","vista mar"])

def es_edificable(t):
    return any(k in str(t).lower() for k in [
        "urbanizable","edificable","construible","licencia construccion"])

def lid(lst): return hashlib.md5(lst["url"].encode()).hexdigest()

def get(url, timeout=8):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  Error {url[:55]}: {e}")
        return None

def listing(fuente, titulo, precio_t, sup_t, region, url, txt, tipo):
    return {
        "fuente": fuente,
        "titulo": (titulo or "Masia/Finca")[:90],
        "precio_t": precio_t or "N/D",
        "precio_n": extraer_precio(precio_t),
        "ha": extraer_ha(sup_t + " " + txt),
        "sup_t": sup_t or "N/D",
        "region": region, "url": url, "tipo": tipo,
        "mar":       tiene_mar(txt),
        "agua":      tiene_agua(txt),
        "luz":       tiene_luz(txt),
        "edificio":  tiene_edificio(titulo+" "+txt),
        "edificable":es_edificable(txt),
        "pueblo":    en_pueblo(titulo+" "+txt) and not es_aislada(titulo+" "+txt),
        "fecha":     datetime.now().strftime("%d.%m.%Y"),
    }

# ============================================================
#  SCRAPERS
# ============================================================

def scrape_buscomasia():
    res = []
    urls_anuncios = set()

    paginas = [
        "https://www.buscomasia.com/tarragona/",
        "https://www.buscomasia.com/priorat/",
        "https://www.buscomasia.com/alt-penedes/",
        "https://www.buscomasia.com/baix-camp/",
        "https://www.buscomasia.com/venta/provincia-tarragona/",
    ]

    for pag in paginas:
        soup = get(pag)
        if not soup: continue
        # Links zu einzelnen Angeboten
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("http"):
                href = "https://www.buscomasia.com" + href
            if "/venta/" in href and href.count("/") >= 4:
                urls_anuncios.add(href)
        time.sleep(1.5)

    print(f"  Buscomasia: {len(urls_anuncios)} URLs encontradas")

    for url in list(urls_anuncios)[:50]:
        soup = get(url)
        if not soup: continue
        txt = soup.get_text(" ", strip=True)

        # Titulo
        h1 = soup.select_one("h1")
        titulo = h1.get_text(strip=True) if h1 else url.split("/")[-2].replace("-"," ").title()

        # Precio — patron "90.000 EUR" o direkt im Text
        precio_t = ""
        for pat in [r"[0-9]{1,3}(?:\.[0-9]{3})+\s*[EUR€]", r"[0-9]+\.?[0-9]*\s*[EUR€]"]:
            m = re.search(pat, txt)
            if m:
                precio_t = m.group(0)
                break

        # Superficie — ha oder m2
        sup_t = ""
        for pat in [r"[0-9][0-9.,]*\s*(?:hectareas?|ha)", r"[0-9]{3,}(?:\.[0-9]{3})*\s*m2"]:
            m = re.search(pat, txt.lower())
            if m:
                sup_t = m.group(0)
                break

        # Region
        region = "Tarragona"
        for reg in ["priorat","alt-penedes","baix-camp","penedes"]:
            if reg in url.lower():
                region = reg.replace("-"," ").title()
                break

        tipo = "masia" if tiene_edificio(titulo+" "+txt) else "terreno"
        res.append(listing("Buscomasia", titulo, precio_t, sup_t, region, url, txt, tipo))
        time.sleep(0.8)

    print(f"  Buscomasia: {len(res)} anuncios procesados")
    return res


def scrape_finques():
    res = []
    soup = None
    for suf in ["/find/?buy_op=selling&kind=rural", "/find/?buy_op=selling"]:
        soup = get("https://www.finquesviaaugusta.com"+suf)
        if soup: break
    if not soup: return res
    for a in soup.select("a[href*='/house/']")[:30]:
        href = a.get("href","")
        if not href.startswith("http"): href = "https://www.finquesviaaugusta.com"+href
        txt = a.get_text(" ", strip=True)
        pm = re.search(r"[\d\.]+\s*€", txt)
        res.append(listing("Finques Via Augusta", txt[:80],
            pm.group(0) if pm else "", "", "Tarragona/El Perello", href, txt, "masia"))
    time.sleep(1.5)
    print(f"  Finques Via Augusta: {len(res)}")
    return res


def scrape_idealista():
    res = []
    for com in ["tarragona","priorat-tarragona","terres-de-l-ebre-tarragona"]:
        soup = get(f"https://www.idealista.com/venta-viviendas/{com}/?tipo=finca&precioMaximo={CONFIG['masia_precio_max']}")
        if not soup: continue
        for art in soup.select("article.item")[:20]:
            a = art.select_one("a.item-link")
            if not a: continue
            href = "https://www.idealista.com"+a.get("href","")
            txt = art.get_text(" ", strip=True)
            p = art.select_one(".item-price")
            sz = " ".join(d.get_text(strip=True) for d in art.select(".item-detail"))
            res.append(listing("Idealista", a.get("title",""),
                p.get_text(strip=True) if p else "", sz,
                com.split("-")[0].title(), href, txt, "masia"))
        time.sleep(2)
    print(f"  Idealista: {len(res)}")
    return res


def scrape_kyero():
    res = []
    for reg in ["tarragona","priorat","terres-de-lebre"]:
        soup = get(f"https://www.kyero.com/en/property-for-sale/{reg}?property_type=finca&max_price={CONFIG['masia_precio_max']}")
        if not soup: continue
        for card in soup.select("[data-listing-id],article")[:20]:
            a = card.select_one("a")
            if not a: continue
            href = a.get("href","")
            if not href.startswith("http"): href = "https://www.kyero.com"+href
            txt = card.get_text(" ", strip=True)
            p = card.select_one("[class*='price']")
            t = card.select_one("h2,h3,[class*='title']")
            s = card.select_one("[class*='size'],[class*='area']")
            res.append(listing("Kyero",
                t.get_text(strip=True) if t else "",
                p.get_text(strip=True) if p else "",
                s.get_text(strip=True) if s else "",
                reg.title(), href, txt, "masia"))
        time.sleep(2)
    print(f"  Kyero: {len(res)}")
    return res


def scrape_fotocasa():
    res = []
    soup = get(f"https://www.fotocasa.es/es/comprar/casas-de-campo/tarragona/todas-las-zonas/l?maxPrice={CONFIG['masia_precio_max']}")
    if not soup: return res
    for item in soup.select("article[class*='Card'],div[class*='re-Card']")[:20]:
        a = item.select_one("a")
        if not a: continue
        href = a.get("href","")
        if not href.startswith("http"): href = "https://www.fotocasa.es"+href
        txt = item.get_text(" ", strip=True)
        p = item.select_one("[class*='price'],[class*='Price']")
        t = item.select_one("h2,h3,[class*='title']")
        s = item.select_one("[class*='surface'],[class*='Surface']")
        res.append(listing("Fotocasa",
            t.get_text(strip=True) if t else "",
            p.get_text(strip=True) if p else "",
            s.get_text(strip=True) if s else "",
            "Tarragona", href, txt, "masia"))
    time.sleep(1.5)
    print(f"  Fotocasa: {len(res)}")
    return res

# ============================================================
#  FILTRO Y PUNTUACION
# ============================================================
def puntuar(lst):
    s = 0
    if lst.get("agua"):       s += 30
    if lst.get("edificio"):   s += 25
    if lst.get("mar"):        s += 25
    if lst.get("luz"):        s += 10
    if lst.get("edificable"): s += 5
    if lst.get("ha"):         s += 5
    return min(s, 100)

def filtrar(listings):
    validos = []
    for lst in listings:
        p  = lst.get("precio_n")
        ha = lst.get("ha")
        t  = lst.get("tipo")

        if t == "masia":
            if p and p > CONFIG["masia_precio_max"]: continue
            if lst.get("pueblo"): continue
            if ha and (ha < CONFIG["masia_ha_min"] or ha > CONFIG["masia_ha_max"]): continue

        if t == "terreno":
            if p and p > CONFIG["terreno_precio_max"]: continue
            if ha and ha < CONFIG["terreno_ha_min"]: continue
            if not lst.get("edificio"): continue

        lst["score"] = puntuar(lst)
        validos.append(lst)

    validos.sort(key=lambda x: -x["score"])
    print(f"  Validos tras filtro: {len(validos)}")
    return validos

# ============================================================
#  EMAIL HTML
# ============================================================
COLORES = {
    "Buscomasia": "#7a1515", "Finques Via Augusta": "#14427a",
    "Terrenos.es": "#1a5c1a", "Idealista": "#c0392b",
    "Kyero": "#1a6ea8", "Fotocasa": "#1a8a40",
}

def tag(txt, color, outline=False):
    if outline:
        return (f'<span style="border:1px solid {color};color:{color};border-radius:3px;'
                f'padding:2px 7px;font-size:11px;font-weight:600;margin-right:3px;'
                f'display:inline-block;margin-bottom:3px">{txt}</span>')
    return (f'<span style="background:{color};color:#fff;border-radius:3px;'
            f'padding:2px 7px;font-size:11px;font-weight:700;margin-right:3px;'
            f'display:inline-block;margin-bottom:3px">{txt}</span>')

def email_html(listings):
    masias   = [l for l in listings if l.get("tipo") == "masia"]
    terrenos = [l for l in listings if l.get("tipo") == "terreno"]

    def bloque(items, titulo_bloque, icono, precio_max):
        if not items: return ""
        filas = ""
        for lst in items:
            cf = COLORES.get(lst["fuente"], "#555")
            tags = tag(lst["fuente"], cf)

            if lst.get("mar"):        tags += tag("&#9733; MAR", "#b8860b")
            # Agua
            if lst.get("agua"):       tags += tag("AGUA &#10003;", "#0077b6")
            else:                     tags += tag("AGUA ?", "#e67e22", outline=True)
            # Luz
            if lst.get("luz"):        tags += tag("LUZ &#10003;", "#e67e00")
            else:                     tags += tag("LUZ ?", "#aaa", outline=True)
            # Edificio
            if lst.get("edificio"):   tags += tag("EDIFICIO &#10003;", "#2e7d32")
            if lst.get("edificable"): tags += tag("EDIFICABLE", "#6a0dad")
            # Superficie desconocida
            if not lst.get("ha"):     tags += tag("SUPERFICIE ?", "#999", outline=True)

            precio_str = f"{lst['precio_n']:,} EUR".replace(",",".") if lst.get("precio_n") else lst["precio_t"]
            ha_str = f"{lst['ha']:.1f} ha" if lst.get("ha") else "? ha"
            sc = lst.get("score", 0)
            sc_color = "#27ae60" if sc >= 70 else ("#f39c12" if sc >= 45 else "#c0392b")
            sc_label = "Alto" if sc >= 70 else ("Medio" if sc >= 45 else "Bajo")

            filas += f"""
            <tr>
              <td style="padding:14px 10px;border-bottom:1px solid #ede8e0;vertical-align:top;min-width:260px">
                <div style="margin-bottom:6px">{tags}</div>
                <a href="{lst['url']}" style="color:#1a0a00;font-weight:700;font-size:14px;
                   text-decoration:none;line-height:1.5">{lst['titulo']}</a><br>
                <span style="color:#999;font-size:12px">&#128205; {lst['region']} &nbsp;&middot;&nbsp; {lst['fecha']}</span>
              </td>
              <td style="padding:14px 10px;border-bottom:1px solid #ede8e0;white-space:nowrap;
                   vertical-align:top;font-weight:800;color:#c0392b;font-size:15px">
                {precio_str}
              </td>
              <td style="padding:14px 10px;border-bottom:1px solid #ede8e0;white-space:nowrap;
                   vertical-align:top;font-weight:700;color:#2e7d32;font-size:14px">
                {ha_str}
              </td>
              <td style="padding:14px 10px;border-bottom:1px solid #ede8e0;
                   text-align:center;vertical-align:top;min-width:80px">
                <div style="font-size:20px;font-weight:800;color:{sc_color}">{sc}</div>
                <div style="font-size:10px;color:{sc_color};margin-bottom:8px">{sc_label}</div>
                <a href="{lst['url']}" style="background:#5c3d11;color:#fff;padding:6px 14px;
                   border-radius:4px;text-decoration:none;font-size:12px;white-space:nowrap">Ver &rarr;</a>
              </td>
            </tr>"""

        return f"""
        <div style="margin-bottom:28px">
          <div style="background:#3d2008;padding:12px 22px;border-radius:8px 8px 0 0;
                      display:flex;justify-content:space-between;align-items:center">
            <h2 style="margin:0;color:#e8c97a;font-size:15px;letter-spacing:1px">
              {icono} {titulo_bloque}</h2>
            <span style="color:#c8a050;font-size:12px">{len(items)} resultados &nbsp;&middot;&nbsp; max {precio_max:,} EUR</span>
          </div>
          <table width="100%" cellspacing="0" cellpadding="0"
                 style="border:1px solid #e0d8c8;border-top:none;background:#fff">
            <thead>
              <tr style="background:#f7f2e8">
                <th style="padding:8px 10px;text-align:left;font-size:10px;color:#aaa;text-transform:uppercase">Propiedad</th>
                <th style="padding:8px 10px;text-align:left;font-size:10px;color:#aaa;text-transform:uppercase">Precio</th>
                <th style="padding:8px 10px;text-align:left;font-size:10px;color:#aaa;text-transform:uppercase">Superficie</th>
                <th style="padding:8px 10px;text-align:center;font-size:10px;color:#aaa;text-transform:uppercase">Score</th>
              </tr>
            </thead>
            <tbody>{filas}</tbody>
          </table>
        </div>"""

    fecha   = datetime.now().strftime("%A %d de %B de %Y")
    n_total = len(listings)
    n_mar   = sum(1 for l in listings if l.get("mar"))

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:20px;background:#f0e8d8;font-family:Georgia,serif">
<div style="max-width:760px;margin:0 auto;background:#fff;border-radius:12px;
            overflow:hidden;box-shadow:0 4px 30px rgba(0,0,0,.15)">

  <div style="background:linear-gradient(135deg,#1a0800,#3d1a00);padding:28px 32px">
    <div style="color:#c8a050;font-size:10px;letter-spacing:4px;text-transform:uppercase;margin-bottom:4px">Alerta diaria</div>
    <div style="color:#e8c97a;font-size:30px;font-weight:900;letter-spacing:1px">CASA MUSA</div>
    <div style="color:#d4a843;font-size:17px;margin-top:2px">Masia Alert</div>
    <div style="color:#8a6030;font-size:12px;margin-top:6px">{fecha}</div>
  </div>

  <div style="background:#fdf5e4;border-bottom:2px solid #e8d4a0;padding:0;display:flex">
    <div style="padding:14px 25px;border-right:1px solid #e8d4a0">
      <div style="font-size:28px;font-weight:900;color:#1a0800">{n_total}</div>
      <div style="font-size:11px;color:#888;text-transform:uppercase">nuevos</div>
    </div>
    <div style="padding:14px 25px;border-right:1px solid #e8d4a0">
      <div style="font-size:28px;font-weight:900;color:#1a6ea8">{len(masias)}</div>
      <div style="font-size:11px;color:#888;text-transform:uppercase">masias</div>
    </div>
    <div style="padding:14px 25px;border-right:1px solid #e8d4a0">
      <div style="font-size:28px;font-weight:900;color:#1a5c1a">{len(terrenos)}</div>
      <div style="font-size:11px;color:#888;text-transform:uppercase">terrenos</div>
    </div>
    {"" if not n_mar else f'<div style="padding:14px 25px"><div style="font-size:28px;font-weight:900;color:#b8860b">{n_mar}</div><div style="font-size:11px;color:#888;text-transform:uppercase">mar</div></div>'}
  </div>

  <div style="padding:18px 22px">
    {bloque(masias, "MASIAS EN VENTA", "&#127968;", CONFIG["masia_precio_max"])}
    {bloque(terrenos, "TERRENOS CON EDIFICIO / RUINA", "&#127795;", CONFIG["terreno_precio_max"])}
  </div>

  <div style="padding:12px 22px;background:#f7f0e0;font-size:11px;color:#aaa;border-top:1px solid #e8d4a0">
    Score: AGUA &#10003; +30 &nbsp;&middot;&nbsp; EDIFICIO +25 &nbsp;&middot;&nbsp; MAR +25 &nbsp;&middot;&nbsp; LUZ +10 &nbsp;&middot;&nbsp; EDIFICABLE +5 &nbsp;&middot;&nbsp; SUPERFICIE +5
  </div>
  <!-- LINKS DIRECTOS A PORTALES -->
  <div style="padding:18px 22px;background:#f7f0e0;border-top:2px solid #e8d4a0">
    <div style="font-size:11px;color:#aaa;text-transform:uppercase;letter-spacing:1px;margin-bottom:12px">
      Buscar directamente en los portales
    </div>
    <table width="100%" cellspacing="0" cellpadding="0">
      <tr>
        <td style="padding:4px 6px 4px 0">
          <a href="https://www.idealista.com/venta-viviendas/tarragona-provincia/?tipo=finca&precioMaximo=150000"
             style="display:block;background:#e74c3c;color:#fff;text-align:center;padding:9px 8px;
             border-radius:5px;text-decoration:none;font-size:12px;font-weight:700">
            Idealista<br><span style="font-weight:400;font-size:10px">Fincas Tarragona</span>
          </a>
        </td>
        <td style="padding:4px 6px 4px 0">
          <a href="https://www.fotocasa.es/es/comprar/casas-de-campo/tarragona/todas-las-zonas/l?maxPrice=150000&minSurface=50000"
             style="display:block;background:#27ae60;color:#fff;text-align:center;padding:9px 8px;
             border-radius:5px;text-decoration:none;font-size:12px;font-weight:700">
            Fotocasa<br><span style="font-weight:400;font-size:10px">Casas de Campo</span>
          </a>
        </td>
        <td style="padding:4px 6px 4px 0">
          <a href="https://www.kyero.com/es/propiedades-en-venta/tarragona?property_type=finca&max_price=150000"
             style="display:block;background:#2980b9;color:#fff;text-align:center;padding:9px 8px;
             border-radius:5px;text-decoration:none;font-size:12px;font-weight:700">
            Kyero<br><span style="font-weight:400;font-size:10px">Fincas Tarragona</span>
          </a>
        </td>
        <td style="padding:4px 6px 4px 0">
          <a href="https://www.habitaclia.com/comprar-masia-en-tarragona.htm?orden=precio_asc&precio_max=150000"
             style="display:block;background:#8e44ad;color:#fff;text-align:center;padding:9px 8px;
             border-radius:5px;text-decoration:none;font-size:12px;font-weight:700">
            Habitaclia<br><span style="font-weight:400;font-size:10px">Masias Tarragona</span>
          </a>
        </td>
        <td style="padding:4px 0">
          <a href="https://www.milanuncios.com/inmobiliaria/?q=finca+rustica+tarragona&orden=fecha"
             style="display:block;background:#e67e22;color:#fff;text-align:center;padding:9px 8px;
             border-radius:5px;text-decoration:none;font-size:12px;font-weight:700">
            Milanuncios<br><span style="font-weight:400;font-size:10px">Fincas Tarragona</span>
          </a>
        </td>
      </tr>
      <tr>
        <td style="padding:4px 6px 4px 0">
          <a href="https://www.buscomasia.com/tarragona/"
             style="display:block;background:#7a1515;color:#fff;text-align:center;padding:9px 8px;
             border-radius:5px;text-decoration:none;font-size:12px;font-weight:700">
            Buscomasia<br><span style="font-weight:400;font-size:10px">Tarragona</span>
          </a>
        </td>
        <td style="padding:4px 6px 4px 0">
          <a href="https://www.buscomasia.com/priorat/"
             style="display:block;background:#7a1515;color:#fff;text-align:center;padding:9px 8px;
             border-radius:5px;text-decoration:none;font-size:12px;font-weight:700">
            Buscomasia<br><span style="font-weight:400;font-size:10px">Priorat</span>
          </a>
        </td>
        <td style="padding:4px 6px 4px 0">
          <a href="https://www.buscomasia.com/baix-ebre/"
             style="display:block;background:#7a1515;color:#fff;text-align:center;padding:9px 8px;
             border-radius:5px;text-decoration:none;font-size:12px;font-weight:700">
            Buscomasia<br><span style="font-weight:400;font-size:10px">Baix Ebre</span>
          </a>
        </td>
        <td style="padding:4px 6px 4px 0">
          <a href="https://www.idealista.com/venta-viviendas/priorat-tarragona/?tipo=finca&precioMaximo=150000"
             style="display:block;background:#e74c3c;color:#fff;text-align:center;padding:9px 8px;
             border-radius:5px;text-decoration:none;font-size:12px;font-weight:700">
            Idealista<br><span style="font-weight:400;font-size:10px">Fincas Priorat</span>
          </a>
        </td>
        <td style="padding:4px 0">
          <a href="https://www.finquesviaaugusta.com/find/?buy_op=selling"
             style="display:block;background:#14427a;color:#fff;text-align:center;padding:9px 8px;
             border-radius:5px;text-decoration:none;font-size:12px;font-weight:700">
            Via Augusta<br><span style="font-weight:400;font-size:10px">Tarragona</span>
          </a>
        </td>
      </tr>
    </table>
  </div>

  <div style="padding:12px 22px;background:#1a0800;text-align:center;font-size:11px;color:#4a2a08">
    CASA MUSA Masia Alert &nbsp;&middot;&nbsp; Tarragona &nbsp;&middot;&nbsp; Priorat &nbsp;&middot;&nbsp; Penedes &nbsp;&middot;&nbsp; Baix Ebre
  </div>
</div>
</body></html>"""

# ============================================================
#  ENVIO
# ============================================================
def enviar_email(listings):
    validos = filtrar(listings) if listings else []
    # Siempre enviar - aunque no haya nuevos scrapeados, el email tiene links utiles
    html    = email_html(validos)
    n_mar   = sum(1 for l in validos if l.get("mar"))
    asunto  = (f"CASA MUSA Masia Alert — {len(validos)} nuevos"
               + (f" · {n_mar}x vistas al mar" if n_mar else "")
               + f" · {datetime.now().strftime('%d.%m.%Y')}")
    empfaenger = [e.strip() for e in CONFIG["email_empfaenger"].split(",")]
    msg = MIMEMultipart("alternative")
    msg["Subject"] = asunto
    msg["From"]    = CONFIG["email_absender"]
    msg["To"]      = ", ".join(empfaenger)
    msg.attach(MIMEText(html, "html"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
        srv.login(CONFIG["email_absender"], CONFIG["email_passwort"])
        srv.sendmail(CONFIG["email_absender"], empfaenger, msg.as_string())
    print(f"  Email enviado: {len(validos)} anuncios")

# ============================================================
#  PRINCIPAL
# ============================================================
def cargar_vistos():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f: return set(json.load(f))
    return set()

def guardar_vistos(ids):
    with open(SEEN_FILE, "w") as f: json.dump(list(ids), f)

def recopilar():
    todos = []
    todos += scrape_buscomasia()
    todos += scrape_finques()
    todos += scrape_idealista()
    todos += scrape_kyero()
    todos += scrape_fotocasa()
    urls, unicos = set(), []
    for l in todos:
        if l["url"] not in urls:
            urls.add(l["url"]); unicos.append(l)
    print(f"  Total unicos: {len(unicos)}")
    return unicos

def ejecutar():
    print(f"\n{'='*55}\n  CASA MUSA · {datetime.now().strftime('%d.%m.%Y %H:%M')}\n{'='*55}")
    vistos = cargar_vistos()
    todos  = recopilar()
    nuevos = [(lid(l), l) for l in todos if lid(l) not in vistos]
    print(f"  Nuevos: {len(nuevos)}")
    if nuevos:
        enviar_email([l for _,l in nuevos])
        guardar_vistos(vistos | {i for i,_ in nuevos})
    else:
        print("  Sin nuevos anuncios.")

if __name__ == "__main__":
    print("CASA MUSA — Masia Alert")
    ejecutar()
