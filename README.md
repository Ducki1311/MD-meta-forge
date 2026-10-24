# MD Meta Forge

Meta-Deckbau für Yu-Gi-Oh! Master Duel: Konsenslisten aus aktuellen Top-Decks, exakte Hand-Wahrscheinlichkeiten, Budget-Filter, Playbook, Deck Doctor und Export (.ydk / ydke:// / Text).

## Funktionen
- **Sprache DE/EN:** Umschalter oben rechts. Auf Deutsch kommen auch Kartennamen und -texte auf Deutsch (YGOPRODeck). Export und Kombo-Analyse laufen intern weiter mit den englischen Daten.
- **Combo-Guide (kostenlos):** Kombo-Baum „wer holt wen?“ direkt aus den Kartentexten, Handtrap-Chokepoints, Links zur MDM-Deckseite und zu YouTube-Combo-Videos.
- **Eigene Guides:** Im Guide auf „Guide schreiben / bearbeiten“. „Als Entwurf speichern“ gilt nur im eigenen Browser. Für alle Besucher: „combos.json herunterladen“ → Datei im GitHub-Repo ersetzen → Render baut automatisch neu.
- **Teilen:** Link mit eingebautem Deck (funktioniert dauerhaft, keine Datenbank nötig) und automatischem Vorschaubild für Discord, Reddit, WhatsApp & Co.

## Lokal starten
```bash
pip install -r requirements.txt
python main.py            # → http://localhost:8000
```
Beim ersten Start lädt die App die Meta-Daten automatisch (ca. 30–90 s). Danach alle 6 Stunden.

## Umgebungsvariablen
| Variable | Zweck | Standard |
|---|---|---|
| `REFRESH_TOKEN` | Geheimnis für den Webhook `/api/refresh` | leer = Webhook aus |
| `REFRESH_HOURS` | Intervall des Auto-Updates | `6` |
| `META_WINDOW_DAYS` | Zeitfenster der berücksichtigten Decks | `30` |
| `ANTHROPIC_API_KEY` | optional: KI-Kombos & KI-Deck-Doctor | leer = aus |
| `ANTHROPIC_MODEL` | Modell für die KI-Funktionen | `claude-sonnet-5-5` |
| `CONTACT_EMAIL` | wird im User-Agent an die Datenquellen gesendet | `unknown` |
| `DATA_DIR` | Ort für SQLite-Cache und Kartenbilder | `./data` |
| `PUBLIC_URL` | optional: eigene Domain für Teilen-Links, z. B. `https://meine-domain.de` | automatisch |

Webhook manuell auslösen: `curl -X POST https://DEINE-APP/api/refresh -H "X-Refresh-Token: …"`

## Hosting

### Render.com (empfohlen, kostenlos)
1. Projekt in ein GitHub-Repo pushen.
2. Render → **New → Blueprint** → Repo wählen. `render.yaml` wird erkannt, `REFRESH_TOKEN` wird automatisch erzeugt.
3. Fertig – die URL steht im Dashboard.

**Free-Tier-Einschränkungen:** Der Dienst schläft nach 15 Minuten ohne Anfragen ein, der erste Aufruf danach dauert rund eine Minute. Die Festplatte ist nicht dauerhaft – nach dem Aufwachen baut die App ihren Cache automatisch neu auf.
Damit die Updates trotzdem regelmäßig laufen: In GitHub unter *Settings → Secrets → Actions* `APP_URL` (z. B. `https://md-meta-forge.onrender.com`) und `REFRESH_TOKEN` (aus Render kopieren) anlegen. Der Workflow `.github/workflows/refresh.yml` weckt die App dann alle 6 Stunden und stößt das Update an.
Echtes 24/7 ohne Kaltstart gibt es bei Render erst mit einer bezahlten Instanz.

### Replit
1. Neues Repl → *Import from GitHub*. `.replit` ist vorkonfiguriert, **Run** startet die App.
2. Dauerbetrieb nur über **Deploy → Reserved VM** (kostenpflichtig). Die kostenlose Workspace-Ausführung stoppt, wenn du das Repl schließt.

### Vercel – nicht geeignet
Vercel führt Python nur als kurzlebige Serverless-Funktionen aus: kein Hintergrund-Scheduler, keine beschreibbare dauerhafte Festplatte für SQLite und Bild-Cache. Für diese App deshalb Render (oder Railway/Fly.io/einen kleinen VPS) verwenden.

## Hinweise zu den Datenquellen
- **masterduelmeta.com** hat keine offiziell dokumentierte öffentliche API. Die App nutzt deren JSON-Endpunkte schonend (alle 6 h) und arbeitet weiter mit dem letzten Cache, falls sie sich ändern. Bei öffentlicher Nutzung bitte das MDM-Team kontaktieren und Quellen nennen.
- **YGOPRODeck**: max. 20 Anfragen/Sekunde; Bilder dürfen nicht dauerhaft hotgelinkt werden. Deshalb lädt `/img/{id}.jpg` jedes Bild genau einmal und liefert es danach lokal aus.
- Win-Rates werden nicht erfunden: Angezeigt wird der **Meta-Anteil** an Top-/Turnier-Decks; offizielle Tier-Badges nur, wenn MDM sie liefert.
- Rollen (Starter/Extender/…) erkennt die App heuristisch aus Kartentexten. Fehler korrigierst du per Klick auf die Karte. Verifizierte Kombos kannst du in `combos.json` hinterlegen.
