name: Masía Tracker

on:
  schedule:
    # Läuft alle 4 Stunden (UTC Zeit)
    - cron: '0 */4 * * *'
  workflow_dispatch:  # Manuell starten möglich (Button in GitHub)

jobs:
  run-tracker:
    runs-on: ubuntu-latest

    steps:
      - name: 📥 Code auschecken
        uses: actions/checkout@v4

      - name: 🐍 Python einrichten
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: 📦 Abhängigkeiten installieren
        run: pip install requests beautifulsoup4

      - name: 💾 Gesehene Angebote laden (Cache)
        uses: actions/cache@v4
        with:
          path: masia_gesehen.json
          key: masia-gesehen-${{ github.run_id }}
          restore-keys: |
            masia-gesehen-

      - name: 🏡 Tracker ausführen
        env:
          EMAIL_ABSENDER:   ${{ secrets.EMAIL_ABSENDER }}
          EMAIL_PASSWORT:   ${{ secrets.EMAIL_PASSWORT }}
          EMAIL_EMPFAENGER: ${{ secrets.EMAIL_EMPFAENGER }}
        run: python masia_tracker.py
