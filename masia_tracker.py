"""
🏡 Masía Tracker — Agrotourismus Edition
Quellen: Buscomasia, Finques Via Augusta, Idealista, Kyero, Fotocasa, Terrenos.es
Kriterien: Tarragona / Priorat / Penedès / Baix Ebre  ·  max 150.000€  ·  min 10ha  ·  Wasser Pflicht

Setup:
    pip install requests beautifulsoup4

Anpassen und starten:
    python masia_tracker.py
"""

import json, os, smtplib, time, hashlib, re
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
from bs4 import BeautifulSoup

# ============================================================
#  DEINE EINSTELLUNGEN — hier alles anpassen!
# ============================================================

CONFIG = {
    # --- E-Mail ---
    # Lokal: hier direkt eintragen
    # GitHub: werden automatisch aus den Repository Secrets gelesen
    "email_absender":   os.getenv("EMAIL_ABSENDER",   "deine.email@gmail.com"),
    "email_passwort":   os.getenv("EMAIL_PASSWORT",   "DEIN_APP_PASSWORT"),
    "email_empfaenger": os.getenv("EMAIL_EMPFAENGER", "empfaenger@beispiel.com"),

    # --- Kriterien ---
    "preis_max":         150_000,   # € Maximalpreis
    "flaeche_min_ha":    10,        # Minimum 10 Hektar (= 100.000 m²)
    "wasser_pflicht":    True,      # Nur mit bestätigtem Wasseranschluss
    "meerblick_bonus":   True,      # Meerblick = Extra-Markierung und +Score
    "bebaubar_bevorzugt": True,     # Suelo urbanizable bevorzugt

    # --- Regionen ---
    "regionen_buscomasia": [
        "tarragona", "priorat", "baix-ebre",
        "alt-penedes", "baix-camp",
    ],
    "region_keywords": [
        "tarragona", "priorat", "penedès", "penedes",
        "baix ebre", "baix camp", "terra alta", "ribera d'ebre",
        "montsià",
    ],

    # --- Gebäude-Schlüsselwörter ---
    "gebaeude_keywords": [
        "masia", "masía", "masoveria", "masovería",
        "casa", "cabaña", "ruina", "ruïna", "edificio",
        "habitable", "construcción", "finca con casa",
    ],

    # --- Intervall ---
    "intervall_minuten": 240,   # alle 4 Stunden
}

SEEN_FILE = "masia_gesehen.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,ca;q=0.8",
}

# ============================================================
#  HILFS-FUNKTIONEN
# ============================================================

def extrahiere_preis(text):
    if not text:
        return None
    for z in re.findall(r"[\d\.]+", text.replace(",", ".")):
        try:
            val = int(float(z.replace(".", "")))
            if 1_000 < val < 10_000_000:
                return val
        except:
            pass
    return None

def extrahiere_flaeche_ha(text):
    if not text:
        return None
    text_low = text.lower()
    # Direkt in ha
    m = re.search(r"([\d\.,]+)\s*ha", text_low)
    if m:
        try:
            return float(m.group(1).replace(",", "."))
        except:
            pass
    # In m² → umrechnen
    m = re.search(r"([\d\.,]+)\s*m[\s²2]", text_low)
    if m:
        try:
            m2 = float(m.group(1).replace(".", "").replace(",", "."))
            if m2 > 1000:  # Mindest-Grundstück
                return m2 / 10_000
        except:
            pass
    return None

def hat_meerblick(text):
    kws = ["mar", "vista al mar", "vistas al mar", "sea view",
           "vista mar", "mediterráneo", "costa", "primera línea"]
    tl = text.lower()
    return any(k in tl for k in kws)

def hat_wasser(text):
    kws = ["agua", "water", "pozo", "fuente", "manantial",
           "suministro agua", "red de agua", "abasteciment", "agua potable"]
    return any(k in text.lower() for k in kws)

def hat_gebaeude(text):
    return any(k in text.lower() for k in CONFIG["gebaeude_keywords"])

def ist_bebaubar(text):
    kws = ["urbanizable", "edificable", "construible",
           "suelo urbano", "licencia construcción"]
    return any(k in text.lower() for k in kws)

def listing_id(lst):
    return hashlib.md5(lst["url"].encode()).hexdigest()

def get(url, timeout=8):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  Fehler {url[:60]}: {e}")
        return None

def make_listing(quelle, titel, preis_text, groesse_text, region, url, text_gesamt):
    return {
        "quelle":       quelle,
        "titel":        (titel or "Masía / Finca")[:90],
        "preis_text":   preis_text or "N/A",
        "preis_num":    extrahiere_preis(preis_text),
        "flaeche_ha":   extrahiere_flaeche_ha(groesse_text + " " + text_gesamt),
        "groesse_text": groesse_text or "N/A",
        "region":       region,
        "url":          url,
        "meerblick":    hat_meerblick(text_gesamt),
        "wasser":       hat_wasser(text_gesamt),
        "gebaeude":     hat_gebaeude(titel + " " + text_gesamt),
        "bebaubar":     ist_bebaubar(text_gesamt),
    }

# ============================================================
#  SCRAPER
# ============================================================

def scrape_buscomasia():
    results = []
    for region in CONFIG["regionen_buscomasia"]:
        url = f"https://www.buscomasia.com/{region}/"
        soup = get(url)
        if not soup:
            continue
        links = soup.select("a[href*='/venta/']")
        for link in links[:25]:
            href = link.get("href", "")
            if not href.startswith("http"):
                href = "https://www.buscomasia.com" + href
            txt = link.get_text(" ", strip=True)
            title_el = link.select_one("h2, h3, [class*='title']")
            price_el  = link.select_one("[class*='price'], [class*='precio']")
            size_el   = link.select_one("[class*='size'], [class*='area']")
            results.append(make_listing(
                "Buscomasia",
                title_el.get_text(strip=True) if title_el else txt[:80],
                price_el.get_text(strip=True) if price_el else "",
                size_el.get_text(strip=True) if size_el else "",
                region.replace("-", " ").title(),
                href, txt
            ))
        time.sleep(1.5)
    print(f"  Buscomasia: {len(results)} gefunden")
    return results


def scrape_finques_via_augusta():
    results = []
    for suffix in ["/find/?buy_op=selling&kind=rural", "/find/?buy_op=selling"]:
        soup = get("https://www.finquesviaaugusta.com" + suffix)
        if soup:
            break
    if not soup:
        return results

    for link in soup.select("a[href*='/house/']")[:30]:
        href = link.get("href", "")
        if not href.startswith("http"):
            href = "https://www.finquesviaaugusta.com" + href
        txt = link.get_text(" ", strip=True)
        # Preis im Text suchen
        pm = re.search(r"[\d\.]+\s*€", txt)
        results.append(make_listing(
            "Finques Via Augusta",
            txt[:80],
            pm.group(0) if pm else "",
            "",
            "Tarragona / El Perelló",
            href, txt
        ))
    time.sleep(1.5)
    print(f"  Finques Via Augusta: {len(results)} gefunden")
    return results


def scrape_terrenos():
    results = []
    urls = [
        "https://tarragona.terrenos.es/finca-rustica/",
        "https://www.terrenos.es/terrenos/tarragona/",
    ]
    for url in urls:
        soup = get(url)
        if not soup:
            continue
        for card in soup.select("article, div[class*='listing']")[:20]:
            link = card.select_one("a")
            if not link:
                continue
            href = link.get("href", "")
            if not href.startswith("http"):
                href = "https://www.terrenos.es" + href
            txt = card.get_text(" ", strip=True)
            price_el = card.select_one("[class*='price'], [class*='precio']")
            size_el  = card.select_one("[class*='size'], [class*='area']")
            title_el = card.select_one("h2, h3")
            results.append(make_listing(
                "Terrenos.es",
                title_el.get_text(strip=True) if title_el else "",
                price_el.get_text(strip=True) if price_el else "",
                size_el.get_text(strip=True) if size_el else "",
                "Tarragona",
                href, txt
            ))
        time.sleep(1.5)
    print(f"  Terrenos.es: {len(results)} gefunden")
    return results


def scrape_idealista():
    results = []
    comarcas = ["tarragona", "priorat-tarragona", "terres-de-l-ebre-tarragona"]
    for comarca in comarcas:
        url = (
            f"https://www.idealista.com/venta-viviendas/{comarca}/"
            f"?tipo=finca&precioMaximo={CONFIG['preis_max']}"
        )
        soup = get(url)
        if not soup:
            continue
        for art in soup.select("article.item")[:20]:
            link = art.select_one("a.item-link")
            if not link:
                continue
            href  = "https://www.idealista.com" + link.get("href", "")
            titel = link.get("title", "")
            txt   = art.get_text(" ", strip=True)
            price = art.select_one(".item-price")
            size  = " ".join(d.get_text(strip=True) for d in art.select(".item-detail"))
            results.append(make_listing(
                "Idealista", titel,
                price.get_text(strip=True) if price else "",
                size,
                comarca.split("-")[0].title(),
                href, txt
            ))
        time.sleep(2)
    print(f"  Idealista: {len(results)} gefunden")
    return results


def scrape_kyero():
    results = []
    regionen = ["tarragona", "priorat", "terres-de-lebre"]
    for region in regionen:
        url = (
            f"https://www.kyero.com/en/property-for-sale/{region}"
            f"?property_type=finca&max_price={CONFIG['preis_max']}"
        )
        soup = get(url)
        if not soup:
            continue
        for card in soup.select("[data-listing-id], article")[:20]:
            link = card.select_one("a")
            if not link:
                continue
            href = link.get("href", "")
            if not href.startswith("http"):
                href = "https://www.kyero.com" + href
            txt = card.get_text(" ", strip=True)
            price = card.select_one("[class*='price']")
            title = card.select_one("h2, h3, [class*='title']")
            size  = card.select_one("[class*='size'], [class*='area']")
            results.append(make_listing(
                "Kyero",
                title.get_text(strip=True) if title else "",
                price.get_text(strip=True) if price else "",
                size.get_text(strip=True) if size else "",
                region.title(),
                href, txt
            ))
        time.sleep(2)
    print(f"  Kyero: {len(results)} gefunden")
    return results


def scrape_fotocasa():
    results = []
    url = (
        f"https://www.fotocasa.es/es/comprar/casas-de-campo/tarragona/"
        f"todas-las-zonas/l?maxPrice={CONFIG['preis_max']}"
    )
    soup = get(url)
    if not soup:
        return results
    for item in soup.select("article[class*='Card'], div[class*='re-Card']")[:20]:
        link = item.select_one("a")
        if not link:
            continue
        href = link.get("href", "")
        if not href.startswith("http"):
            href = "https://www.fotocasa.es" + href
        txt   = item.get_text(" ", strip=True)
        price = item.select_one("[class*='price'], [class*='Price']")
        title = item.select_one("h2, h3, [class*='title']")
        size  = item.select_one("[class*='surface'], [class*='Surface']")
        results.append(make_listing(
            "Fotocasa",
            title.get_text(strip=True) if title else "",
            price.get_text(strip=True) if price else "",
            size.get_text(strip=True) if size else "",
            "Tarragona",
            href, txt
        ))
    time.sleep(1.5)
    print(f"  Fotocasa: {len(results)} gefunden")
    return results

# ============================================================
#  FILTER & SCORING
# ============================================================

def score_listing(lst):
    s = 50
    if lst.get("meerblick"):  s += 25
    if lst.get("bebaubar"):   s += 10
    if lst.get("wasser"):     s += 10
    if lst.get("gebaeude"):   s +=  5
    if lst.get("flaeche_ha"): s +=  5
    return s

def wende_filter_an(listings):
    qualifiziert = []
    for lst in listings:
        p = lst.get("preis_num")
        ha = lst.get("flaeche_ha")
        if p and p > CONFIG["preis_max"]:
            continue
        if ha and ha < CONFIG["flaeche_min_ha"]:
            continue
        lst["flaeche_warnung"] = (ha is None)
        lst["wasser_unbekannt"] = (not lst.get("wasser"))
        lst["score"] = score_listing(lst)
        qualifiziert.append(lst)
    qualifiziert.sort(key=lambda x: -x["score"])
    print(f"  Nach Filter: {len(qualifiziert)} qualifiziert")
    return qualifiziert

# ============================================================
#  E-MAIL
# ============================================================

FARBEN = {
    "Buscomasia":          "#8b1a1a",
    "Finques Via Augusta":  "#1a5c8b",
    "Terrenos.es":          "#2d6a2d",
    "Idealista":            "#e74c3c",
    "Kyero":                "#2980b9",
    "Fotocasa":             "#27ae60",
}

def badge(text, farbe):
    return (f'<span style="background:{farbe};color:#fff;border-radius:4px;'
            f'padding:2px 7px;font-size:11px;font-weight:bold;margin-right:3px">{text}</span>')

def baue_email_html(listings):
    zeilen = ""
    for lst in listings:
        farbe  = FARBEN.get(lst["quelle"], "#555")
        badges = badge(lst["quelle"], farbe)
        if lst.get("meerblick"):        badges += badge("⭐ Meerblick", "#c8950a")
        if lst.get("bebaubar"):         badges += badge("🏗️ Bebaubar", "#6a0dad")
        if lst.get("wasser"):           badges += badge("💧 Wasser ✓", "#0077b6")
        elif lst.get("wasser_unbekannt"): badges += badge("💧 Wasser?", "#aaa")
        if lst.get("gebaeude"):         badges += badge("🏠 Gebäude", "#4a6741")
        if lst.get("flaeche_warnung"):  badges += badge("⚠️ Größe?", "#cc7a00")

        preis_str  = (f"{lst['preis_num']:,} €".replace(",", ".")
                      if lst.get("preis_num") else lst["preis_text"])
        flaeche_str = (f"{lst['flaeche_ha']:.1f} ha"
                       if lst.get("flaeche_ha") else "— ha")
        sc = lst.get("score", 0)
        sc_farbe = "#27ae60" if sc >= 75 else ("#f39c12" if sc >= 60 else "#e74c3c")

        zeilen += f"""
        <tr>
          <td style="padding:15px 8px;border-bottom:1px solid #f0ede8;vertical-align:top">
            <div style="margin-bottom:5px">{badges}</div>
            <a href="{lst['url']}" style="color:#2c2415;font-weight:700;font-size:14px;
               text-decoration:none;line-height:1.5">{lst['titel']}</a><br>
            <span style="color:#999;font-size:12px">📍 {lst['region']}</span>
          </td>
          <td style="padding:15px 8px;border-bottom:1px solid #f0ede8;
               white-space:nowrap;font-weight:800;color:#c0392b;font-size:16px;vertical-align:top">
            {preis_str}
          </td>
          <td style="padding:15px 8px;border-bottom:1px solid #f0ede8;
               white-space:nowrap;color:#444;font-size:13px;vertical-align:top">
            🌿 {flaeche_str}
          </td>
          <td style="padding:15px 8px;border-bottom:1px solid #f0ede8;
               text-align:center;vertical-align:top">
            <div style="color:{sc_farbe};font-weight:800;font-size:20px">{sc}</div>
            <div style="font-size:10px;color:#bbb;margin-bottom:6px">Score</div>
            <a href="{lst['url']}" style="background:#8b6914;color:#fff;padding:6px 12px;
               border-radius:5px;text-decoration:none;font-size:12px">Ansehen →</a>
          </td>
        </tr>"""

    datum  = datetime.now().strftime("%A, %d. %B %Y · %H:%M Uhr")
    n      = len(listings)
    meerk  = sum(1 for l in listings if l.get("meerblick"))
    tops   = max((l.get("score", 0) for l in listings), default=0)

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f0ebe1;font-family:Georgia,serif">
<div style="max-width:740px;margin:30px auto;background:#fff;border-radius:12px;
            overflow:hidden;box-shadow:0 4px 30px rgba(0,0,0,.15)">

  <div style="background:linear-gradient(135deg,#2c1810,#4a2c1a);padding:35px 40px">
    <h1 style="margin:0;color:#d4a843;font-size:26px;letter-spacing:2px">
      🏡 Masía Agrotourismus Alert</h1>
    <p style="margin:8px 0 0;color:#c8a870;font-size:13px">{datum}</p>
  </div>

  <div style="display:flex;gap:0;background:#fdf6e8;border-bottom:2px solid #e8d9b0">
    <div style="padding:16px 30px;border-right:1px solid #e8d9b0">
      <div style="font-size:28px;font-weight:800;color:#2c1810">{n}</div>
      <div style="font-size:12px;color:#888">neue Angebote</div>
    </div>
    {"" if not meerk else f'<div style="padding:16px 30px;border-right:1px solid #e8d9b0"><div style="font-size:28px;font-weight:800;color:#c8950a">{meerk}</div><div style="font-size:12px;color:#888">Meerblick ⭐</div></div>'}
    <div style="padding:16px 30px">
      <div style="font-size:28px;font-weight:800;color:#27ae60">{tops}</div>
      <div style="font-size:12px;color:#888">Top-Score</div>
    </div>
  </div>

  <div style="padding:10px 30px;background:#f7f3ec;font-size:12px;color:#777;
              border-bottom:1px solid #e8d9b0">
    🎯 Max <strong>{CONFIG["preis_max"]:,}€</strong> ·
    Min <strong>{CONFIG["flaeche_min_ha"]} ha</strong> ·
    💧 Wasser <strong>Pflicht</strong> ·
    ⭐ Meerblick Bonus &nbsp;|&nbsp;
    📍 Tarragona · Priorat · Penedès · Baix Ebre · Terra Alta
  </div>

  <div style="padding:10px 15px 20px">
    <table width="100%" cellspacing="0" cellpadding="0">
      <thead>
        <tr style="background:#f7f3ec">
          <th style="padding:9px 8px;text-align:left;font-size:10px;color:#aaa;
              text-transform:uppercase">Inserat</th>
          <th style="padding:9px 8px;text-align:left;font-size:10px;color:#aaa;
              text-transform:uppercase">Preis</th>
          <th style="padding:9px 8px;text-align:left;font-size:10px;color:#aaa;
              text-transform:uppercase">Fläche</th>
          <th style="padding:9px 8px;text-align:center;font-size:10px;color:#aaa;
              text-transform:uppercase">Score / Link</th>
        </tr>
      </thead>
      <tbody>{zeilen}</tbody>
    </table>
  </div>

  <div style="padding:14px 30px;background:#fdf6e8;font-size:11px;color:#aaa;
              border-top:1px solid #e8d9b0">
    <strong>Score:</strong>
    Meerblick +25 · Bebaubar +10 · Wasser +10 · Gebäude +5 · Fläche bekannt +5
  </div>
  <div style="padding:16px 30px;background:#2c1810;text-align:center;
              font-size:11px;color:#7a5a3a">
    Buscomasia · Finques Via Augusta · Terrenos.es · Idealista · Kyero · Fotocasa
  </div>
</div>
</body></html>"""


def sende_email(neue_listings):
    gefiltert = wende_filter_an(neue_listings)
    if not gefiltert:
        print("  Keine qualifizierten Angebote nach Filterung.")
        return
    html    = baue_email_html(gefiltert)
    meerk   = sum(1 for l in gefiltert if l.get("meerblick"))
    betreff = (f"🏡 {len(gefiltert)} neue Masías"
               + (f" · {meerk}x Meerblick ⭐" if meerk else "")
               + f" – {datetime.now().strftime('%d.%m.%Y')}")
    # Mehrere Empfänger: kommagetrennt im Secret
    empfaenger_liste = [e.strip() for e in CONFIG["email_empfaenger"].split(",")]
    msg = MIMEMultipart("alternative")
    msg["Subject"] = betreff
    msg["From"]    = CONFIG["email_absender"]
    msg["To"]      = ", ".join(empfaenger_liste)
    msg.attach(MIMEText(html, "html"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
        srv.login(CONFIG["email_absender"], CONFIG["email_passwort"])
        srv.sendmail(CONFIG["email_absender"], empfaenger_liste, msg.as_string())
    print(f"  E-Mail gesendet: {len(gefiltert)} qualifizierte Angebote")

# ============================================================
#  HAUPTSCHLEIFE
# ============================================================

def lade_gesehene():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()

def speichere_gesehene(ids):
    with open(SEEN_FILE, "w") as f:
        json.dump(list(ids), f)

def sammle_alle():
    alle = []
    alle += scrape_buscomasia()
    alle += scrape_finques_via_augusta()
    alle += scrape_terrenos()
    alle += scrape_idealista()
    alle += scrape_kyero()
    alle += scrape_fotocasa()
    seen_urls, unique = set(), []
    for l in alle:
        if l["url"] not in seen_urls:
            seen_urls.add(l["url"])
            unique.append(l)
    print(f"  Gesamt: {len(unique)} einzigartige Inserate")
    return unique

def pruefe_und_sende():
    print(f"\n{'='*60}\n  Masía Tracker · {datetime.now().strftime('%d.%m.%Y %H:%M')}\n{'='*60}")
    gesehen  = lade_gesehene()
    alle     = sammle_alle()
    neue_tup = [(listing_id(l), l) for l in alle if listing_id(l) not in gesehen]
    print(f"  Davon neu: {len(neue_tup)}")
    if neue_tup:
        sende_email([l for _, l in neue_tup])
        speichere_gesehene(gesehen | {uid for uid, _ in neue_tup})
    else:
        print("  Keine neuen Inserate.")

def main():
    print("🏡 Masía Tracker — Agrotourismus Edition")
    print(f"   Quellen: Buscomasia, Finques Via Augusta, Terrenos.es, Idealista, Kyero, Fotocasa")
    print(f"   Kriterien: max {CONFIG['preis_max']:,}€ · min {CONFIG['flaeche_min_ha']}ha · Wasser Pflicht")
    print(f"   E-Mail an: {CONFIG['email_empfaenger']}\n")
    pruefe_und_sende()
    while True:
        print(f"\n  Nächste Prüfung in {CONFIG['intervall_minuten']} Minuten …")
        time.sleep(CONFIG["intervall_minuten"] * 60)
        pruefe_und_sende()

if __name__ == "__main__":
    main()
