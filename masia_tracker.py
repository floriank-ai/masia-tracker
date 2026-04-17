"""
CASA MUSA — Masia Alert
Fuentes: Buscomasia, Finques Via Augusta, Idealista, Kyero, Fotocasa, Terrenos.es
"""

import json, os, smtplib, hashlib, re
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

    # Masias: 5-10 ha, max 150k EUR, sin pueblo, con edificio
    "masia_precio_max":   150_000,
    "masia_ha_min":       5,
    "masia_ha_max":       10,

    # Terrenos: min 10 ha, max 60k EUR, con ruina/casa obligatorio
    "terreno_precio_max": 60_000,
    "terreno_ha_min":     10,

    "regiones_buscomasia": [
        "tarragona", "priorat", "baix-ebre", "alt-penedes", "baix-camp",
    ],
    "palabras_edificio": [
        "masia", "masia", "masoveria", "casa", "cabana", "ruina",
        "edificio", "habitable", "construccion", "finca con casa", "pages",
    ],
    "palabras_pueblo": [
        "casco urbano", "centro urbano", "calle ", "carrer ", "plaza ",
        "placa ", "barrio", "piso", "apartamento", "passeig", "paseo ",
        "en el pueblo", "al pueblo", "al centre", "al centre del",
        "poble", "urban", "urbana",
    ],
    # Palabras clave para confirmar que es AISLADA (freistehend)
    "palabras_aislada": [
        "aislada", "aislado", "finca aislada", "masía aislada",
        "casa aislada", "entorno rural", "campo", "zona rural",
        "finca rustica", "rustico", "rustica", "mas ", "maso",
    ],
}

SEEN_FILE = "masia_gesehen.json"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,ca;q=0.8",
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
    m = re.search(r"([\d\.,]+)\s*ha", tl)
    if m:
        try: return float(m.group(1).replace(",","."))
        except: pass
    m = re.search(r"([\d\.,]+)\s*m[\s2²]", tl)
    if m:
        try:
            v = float(m.group(1).replace(".","").replace(",","."))
            if v > 1000: return round(v/10000, 2)
        except: pass
    return None

def tiene_agua(t):
    kws = ["agua","pozo","fuente","manantial","suministro agua","red de agua","agua potable","agua corriente"]
    return any(k in str(t).lower() for k in kws)

def tiene_edificio(t):
    return any(k in str(t).lower() for k in CONFIG["palabras_edificio"])

def en_pueblo(t):
    return any(k in str(t).lower() for k in CONFIG["palabras_pueblo"])

def tiene_luz(t):
    kws = ["luz", "electricidad", "electrica", "electrico", "corriente electrica",
           "suministro electrico", "red electrica", "placas solares", "solar",
           "fotovoltaica", "autoconsumo", "luz y agua", "agua y luz"]
    return any(k in str(t).lower() for k in kws)

def es_aislada(t):
    """Detecta si la propiedad parece estar aislada/freistehend"""
    return any(k in str(t).lower() for k in CONFIG["palabras_aislada"])

def tiene_mar(t):
    return any(k in str(t).lower() for k in ["vista al mar","vistas al mar","mar ","costa","mediterraneo"])

def es_edificable(t):
    return any(k in str(t).lower() for k in ["urbanizable","edificable","construible","licencia"])

def lid(lst): return hashlib.md5(lst["url"].encode()).hexdigest()

def get(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  Error {url[:55]}: {e}")
        return None

def listing(fuente, titulo, precio_t, sup_t, region, url, txt, tipo):
    return {
        "fuente": fuente, "titulo": (titulo or "Masia/Finca")[:90],
        "precio_t": precio_t or "N/D", "precio_n": extraer_precio(precio_t),
        "ha": extraer_ha(sup_t + " " + txt), "sup_t": sup_t or "N/D",
        "region": region, "url": url, "tipo": tipo,
        "mar": tiene_mar(txt), "agua": tiene_agua(txt),
        "edificio": tiene_edificio(titulo+" "+txt),
        "edificable": es_edificable(txt), "pueblo": en_pueblo(titulo+" "+txt),
        "luz": tiene_luz(txt), "aislada": es_aislada(titulo+" "+txt),
        "fecha": datetime.now().strftime("%d.%m.%Y"),
    }

# ============================================================
#  SCRAPERS
# ============================================================
def scrape_buscomasia():
    res = []
    for reg in CONFIG["regiones_buscomasia"]:
        soup = get(f"https://www.buscomasia.com/{reg}/")
        if not soup: continue
        for a in soup.select("a[href*='/venta/']")[:25]:
            href = a.get("href","")
            if not href.startswith("http"): href = "https://www.buscomasia.com"+href
            txt = a.get_text(" ", strip=True)
            t = a.select_one("h2,h3,[class*='title']")
            p = a.select_one("[class*='price'],[class*='precio']")
            s = a.select_one("[class*='size'],[class*='area'],[class*='superficie']")
            res.append(listing("Buscomasia",
                t.get_text(strip=True) if t else txt[:80],
                p.get_text(strip=True) if p else "",
                s.get_text(strip=True) if s else "",
                reg.replace("-"," ").title(), href, txt, "masia"))
        import time; time.sleep(1.5)
    print(f"  Buscomasia: {len(res)}")
    return res

def scrape_finques():
    res = []
    soup = None
    for suf in ["/find/?buy_op=selling&kind=rural","/find/?buy_op=selling"]:
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
    import time; time.sleep(1.5)
    print(f"  Finques Via Augusta: {len(res)}")
    return res

def scrape_terrenos():
    res = []
    for url in ["https://tarragona.terrenos.es/finca-rustica/","https://www.terrenos.es/terrenos/tarragona/"]:
        soup = get(url)
        if not soup: continue
        for card in soup.select("article,div[class*='listing']")[:20]:
            a = card.select_one("a")
            if not a: continue
            href = a.get("href","")
            if not href.startswith("http"): href = "https://www.terrenos.es"+href
            txt = card.get_text(" ", strip=True)
            p = card.select_one("[class*='price'],[class*='precio']")
            s = card.select_one("[class*='size'],[class*='area']")
            h = card.select_one("h2,h3")
            res.append(listing("Terrenos.es",
                h.get_text(strip=True) if h else "",
                p.get_text(strip=True) if p else "",
                s.get_text(strip=True) if s else "",
                "Tarragona", href, txt, "terreno"))
        import time; time.sleep(1.5)
    print(f"  Terrenos.es: {len(res)}")
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
        import time; time.sleep(2)
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
        import time; time.sleep(2)
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
    import time; time.sleep(1.5)
    print(f"  Fotocasa: {len(res)}")
    return res

# ============================================================
#  FILTRO Y PUNTUACION
# ============================================================
def puntuar(lst):
    s = 0
    if lst.get("agua"):      s += 30  # Agua confirmada
    if lst.get("edificio"):  s += 25  # Edificio/ruina
    if lst.get("mar"):       s += 25  # Vistas al mar
    if lst.get("edificable"):s += 10  # Edificable
    if lst.get("ha"):        s += 10  # Superficie conocida
    return min(s, 100)

def filtrar(listings):
    validos = []
    for lst in listings:
        p  = lst.get("precio_n")
        ha = lst.get("ha")
        t  = lst.get("tipo")

        if t == "masia":
            if p and p > CONFIG["masia_precio_max"]: continue
            # Excluir si detectado en pueblo Y no hay indicios de aislamiento
            if lst.get("pueblo") and not lst.get("aislada"): continue
            if ha and (ha < CONFIG["masia_ha_min"] or ha > CONFIG["masia_ha_max"]): continue

        if t == "terreno":
            if p and p > CONFIG["terreno_precio_max"]: continue
            if ha and ha < CONFIG["terreno_ha_min"]: continue
            if not lst.get("edificio"): continue  # Terreno sin ruina/casa: ignorar

        lst["agua_ok"]  = lst.get("agua", False)
        lst["ha_ok"]    = ha is not None
        lst["score"]    = puntuar(lst)
        validos.append(lst)

    validos.sort(key=lambda x: -x["score"])
    print(f"  Validos tras filtro: {len(validos)}")
    return validos

# ============================================================
#  EMAIL
# ============================================================
COLORES = {
    "Buscomasia": "#7a1515", "Finques Via Augusta": "#14427a",
    "Terrenos.es": "#1a5c1a", "Idealista": "#c0392b",
    "Kyero": "#1a6ea8", "Fotocasa": "#1a8a40",
}

def tag(txt, color, border=False):
    b = f"border:1px solid {color};" if border else ""
    bg = "transparent" if border else color
    fc = color if border else "#fff"
    return (f'<span style="background:{bg};color:{fc};{b}border-radius:3px;'
            f'padding:2px 8px;font-size:11px;font-weight:700;margin-right:3px;'
            f'display:inline-block;margin-bottom:3px">{txt}</span>')

def email_html(listings):
    # Separar masias y terrenos
    masias   = [l for l in listings if l.get("tipo") == "masia"]
    terrenos = [l for l in listings if l.get("tipo") == "terreno"]

    def bloque(items, titulo_bloque, icono):
        if not items: return ""
        filas = ""
        for lst in items:
            cf = COLORES.get(lst["fuente"], "#555")
            tags = tag(lst["fuente"], cf)
            if lst.get("mar"):       tags += tag("★ MAR", "#b8860b")
            if lst.get("agua_ok"):   tags += tag("AGUA ✓", "#0077b6")
            else:                    tags += tag("AGUA ?", "#e67e22")
            if lst.get("edificio"):  tags += tag("EDIFICIO ✓", "#2e7d32")
            if lst.get("edificable"):tags += tag("EDIFICABLE", "#6a0dad")
            if lst.get("luz"):          tags += tag("LUZ ✓", "#e67e00")
            else:                        tags += tag("LUZ ?", "#bbb", border=True)
            if not lst.get("ha_ok"): tags += tag("SUPERFICIE ?", "#999", border=True)

            precio_str = f"{lst['precio_n']:,} EUR".replace(",",".") if lst.get("precio_n") else lst["precio_t"]
            ha_str = f"{lst['ha']:.1f} ha" if lst.get("ha") else "? ha"
            sc = lst.get("score", 0)
            sc_color = "#27ae60" if sc >= 70 else ("#f39c12" if sc >= 45 else "#c0392b")
            sc_label = "Alto" if sc >= 70 else ("Medio" if sc >= 45 else "Bajo")

            filas += f"""
            <tr>
              <td style="padding:14px 10px;border-bottom:1px solid #ede8e0;vertical-align:top">
                <div style="margin-bottom:6px">{tags}</div>
                <a href="{lst['url']}" style="color:#1a0a00;font-weight:700;font-size:14px;
                   text-decoration:none;line-height:1.5">{lst['titulo']}</a><br>
                <span style="color:#999;font-size:12px">&#128205; {lst['region']} &nbsp;·&nbsp; {lst['fecha']}</span>
              </td>
              <td style="padding:14px 10px;border-bottom:1px solid #ede8e0;white-space:nowrap;
                   vertical-align:top;font-weight:800;color:#c0392b;font-size:15px">
                {precio_str}
              </td>
              <td style="padding:14px 10px;border-bottom:1px solid #ede8e0;white-space:nowrap;
                   vertical-align:top;color:#2e7d32;font-weight:700;font-size:14px">
                {ha_str}
              </td>
              <td style="padding:14px 10px;border-bottom:1px solid #ede8e0;text-align:center;vertical-align:top">
                <div style="font-size:18px;font-weight:800;color:{sc_color}">{sc}</div>
                <div style="font-size:10px;color:{sc_color};margin-bottom:6px">{sc_label}</div>
                <a href="{lst['url']}" style="background:#5c3d11;color:#fff;padding:6px 12px;
                   border-radius:4px;text-decoration:none;font-size:12px">Ver &rarr;</a>
              </td>
            </tr>"""

        return f"""
        <div style="margin-bottom:30px">
          <div style="background:#3d2008;padding:12px 25px;border-radius:8px 8px 0 0">
            <h2 style="margin:0;color:#e8c97a;font-size:16px;letter-spacing:1px">
              {icono} {titulo_bloque} <span style="font-size:13px;color:#c8a050">({len(items)} resultados)</span>
            </h2>
          </div>
          <table width="100%" cellspacing="0" cellpadding="0"
                 style="border:1px solid #e0d8c8;border-top:none;border-radius:0 0 8px 8px;overflow:hidden">
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

    fecha = datetime.now().strftime("%A %d de %B de %Y")
    n_total = len(listings)
    n_mar   = sum(1 for l in listings if l.get("mar"))

    criterios = (
        f"<b>Masias:</b> 5–10 ha · max 150.000 EUR · sin pueblo · con edificio &nbsp;|&nbsp; "
        f"<b>Terrenos:</b> min 10 ha · max 60.000 EUR · con ruina/casa"
    )

    score_explicacion = (
        "Puntuacion (0-100): "
        "Agua confirmada +30 &nbsp;·&nbsp; Edificio/ruina +25 &nbsp;·&nbsp; "
        "Vistas al mar +25 &nbsp;·&nbsp; Edificable +10 &nbsp;·&nbsp; Superficie conocida +10"
    )

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f0e8d8;font-family:Georgia,serif">
<div style="max-width:760px;margin:20px auto;background:#fff;border-radius:12px;
            overflow:hidden;box-shadow:0 4px 30px rgba(0,0,0,.15)">

  <!-- CABECERA -->
  <div style="background:linear-gradient(135deg,#1a0800 0%,#3d1a00 100%);padding:30px 35px">
    <div style="color:#c8a050;font-size:11px;letter-spacing:3px;text-transform:uppercase;margin-bottom:6px">
      Alerta diaria
    </div>
    <h1 style="margin:0;color:#e8c97a;font-size:28px;font-weight:900;letter-spacing:1px">
      CASA MUSA
    </h1>
    <div style="color:#d4a843;font-size:18px;margin-top:4px">Masia Alert</div>
    <div style="color:#a07840;font-size:13px;margin-top:8px">{fecha}</div>
  </div>

  <!-- ESTADISTICAS -->
  <div style="background:#fdf5e4;border-bottom:2px solid #e8d4a0;display:flex">
    <div style="padding:16px 28px;border-right:1px solid #e8d4a0">
      <div style="font-size:30px;font-weight:900;color:#1a0800">{n_total}</div>
      <div style="font-size:11px;color:#888;text-transform:uppercase">nuevos anuncios</div>
    </div>
    <div style="padding:16px 28px;border-right:1px solid #e8d4a0">
      <div style="font-size:30px;font-weight:900;color:#1a6ea8">{len(masias)}</div>
      <div style="font-size:11px;color:#888;text-transform:uppercase">masias</div>
    </div>
    <div style="padding:16px 28px;border-right:1px solid #e8d4a0">
      <div style="font-size:30px;font-weight:900;color:#1a5c1a">{len(terrenos)}</div>
      <div style="font-size:11px;color:#888;text-transform:uppercase">terrenos</div>
    </div>
    {"" if not n_mar else f'<div style="padding:16px 28px"><div style="font-size:30px;font-weight:900;color:#b8860b">{n_mar}</div><div style="font-size:11px;color:#888;text-transform:uppercase">vistas al mar</div></div>'}
  </div>

  <!-- FILTROS ACTIVOS -->
  <div style="padding:10px 25px;background:#f7f0e0;font-size:12px;color:#7a5a30;
              border-bottom:1px solid #e8d4a0">
    Filtros activos: {criterios}
  </div>

  <!-- CONTENIDO -->
  <div style="padding:20px 20px">
    {bloque(masias, "MASIAS EN VENTA", "&#127968;")}
    {bloque(terrenos, "TERRENOS CON EDIFICIO", "&#127795;")}
  </div>

  <!-- LEYENDA SCORE -->
  <div style="padding:12px 25px;background:#f7f0e0;font-size:11px;color:#aaa;
              border-top:1px solid #e8d4a0">
    {score_explicacion}
  </div>

  <!-- PIE -->
  <div style="padding:14px 25px;background:#1a0800;text-align:center;font-size:11px;color:#5a3a18">
    Buscomasia &nbsp;·&nbsp; Finques Via Augusta &nbsp;·&nbsp; Terrenos.es &nbsp;·&nbsp;
    Idealista &nbsp;·&nbsp; Kyero &nbsp;·&nbsp; Fotocasa<br>
    <span style="color:#3a2010">CASA MUSA Masia Alert &nbsp;·&nbsp; Tarragona · Priorat · Penedes · Baix Ebre</span>
  </div>
</div>
</body></html>"""

# ============================================================
#  ENVIO EMAIL
# ============================================================
def enviar_email(listings):
    validos = filtrar(listings)
    if not validos:
        print("  Sin resultados validos, no se envia email.")
        return
    html = email_html(validos)
    n_mar = sum(1 for l in validos if l.get("mar"))
    asunto = (
        f"CASA MUSA Masia Alert — {len(validos)} nuevos anuncios"
        + (f" · {n_mar}x vistas al mar" if n_mar else "")
        + f" · {datetime.now().strftime('%d.%m.%Y')}"
    )
    empfaenger = [e.strip() for e in CONFIG["email_empfaenger"].split(",")]
    msg = MIMEMultipart("alternative")
    msg["Subject"] = asunto
    msg["From"]    = CONFIG["email_absender"]
    msg["To"]      = ", ".join(empfaenger)
    msg.attach(MIMEText(html, "html"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
        srv.login(CONFIG["email_absender"], CONFIG["email_passwort"])
        srv.sendmail(CONFIG["email_absender"], empfaenger, msg.as_string())
    print(f"  Email enviado: {len(validos)} anuncios a {', '.join(empfaenger)}")

# ============================================================
#  BUCLE PRINCIPAL
# ============================================================
def cargar_vistos():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f: return set(json.load(f))
    return set()

def guardar_vistos(ids):
    with open(SEEN_FILE, "w") as f: json.dump(list(ids), f)

def recopilar_todo():
    todos = []
    todos += scrape_buscomasia()
    todos += scrape_finques()
    todos += scrape_terrenos()
    todos += scrape_idealista()
    todos += scrape_kyero()
    todos += scrape_fotocasa()
    urls_vistos, unicos = set(), []
    for l in todos:
        if l["url"] not in urls_vistos:
            urls_vistos.add(l["url"])
            unicos.append(l)
    print(f"  Total: {len(unicos)} anuncios unicos")
    return unicos

def ejecutar():
    print(f"\n{'='*55}\n  CASA MUSA · {datetime.now().strftime('%d.%m.%Y %H:%M')}\n{'='*55}")
    vistos   = cargar_vistos()
    todos    = recopilar_todo()
    nuevos   = [(lid(l), l) for l in todos if lid(l) not in vistos]
    print(f"  Nuevos: {len(nuevos)}")
    if nuevos:
        enviar_email([l for _, l in nuevos])
        guardar_vistos(vistos | {i for i, _ in nuevos})
    else:
        print("  Sin nuevos anuncios.")

if __name__ == "__main__":
    print("CASA MUSA — Masia Alert")
    print(f"  Masias: {CONFIG['masia_ha_min']}-{CONFIG['masia_ha_max']} ha · max {CONFIG['masia_precio_max']:,} EUR")
    print(f"  Terrenos: min {CONFIG['terreno_ha_min']} ha · max {CONFIG['terreno_precio_max']:,} EUR · con edificio")
    print(f"  Email a: {CONFIG['email_empfaenger']}\n")
    ejecutar()
