# CLAUDE.md — markitdown-api (Notae)

Contexte et procédure de déploiement pour Claude Code / contributeurs. Pour l'usage
de l'API elle-même, voir `README.md`.

## Ce que c'est

Micro-service **FastAPI** (Python) qui expose `POST /process_file` : reçoit un fichier
binaire, l'extrait en Markdown via la lib [MarkItDown](https://github.com/microsoft/markitdown),
renvoie `{ "markdown": "..." }`. C'est **le** moteur d'extraction de texte de Notae pour
les fichiers bureautiques (xlsx, xls, docx, pptx, pdf, txt, csv, eml…).

- Fork de `dezoito/markitdown-api` (lui-même fork de `elbruno/MarkItDownServer`).
  Remote `upstream` = `dezoito/markitdown-api`, `origin` = `notae-ai/markitdown-api`.
- **Presque tout tient dans `app.py`** (~240 lignes). `requirements.txt` épingle
  `markitdown[all]==0.1.5` (qui tire `openpyxl` en transitif — non épinglé
  explicitement).

### Consommé par le monorepo

Le monorepo Notae appelle ce service en HTTP (jamais en direct la lib Python) :
`packages/lib/markitdown.ts` (`convertWithMarkitdown`) et
`packages/domains/tools/markitdown.service.ts`. URL configurée via l'env
**`MARKITDOWN_API_URL`**. C'est ici (et pas dans le monorepo) qu'on corrige les bugs
de conversion — aucune version openpyxl/markitdown n'est pinnée côté monorepo.

## Convention clé : monkeypatches openpyxl dans `app.py`

Certains exports Excel de sources tierces (ex. documents **Pappers**) produisent des
xlsx techniquement malformés qu'Excel tolère mais qu'openpyxl refuse, ce qui fait
planter toute la conversion. On les rustine par **monkeypatch openpyxl**, placés
**avant** `from markitdown import MarkItDown` (sinon MarkItDown charge openpyxl non
patché). Patchs en place :

- `_safe_cast_number` — tolère `NaN`/`Inf` dans les **valeurs de cellules**.
- `_clean_xlsx_convert` / `_clean_xls_convert` — override `XlsxConverter.convert`,
  lit via `pd.read_excel` et rend `NaN` → cellule vide.
- `_safe_fill_from_tree` — tolère les `<fill/>` **vides** dans `xl/styles.xml` (openpyxl
  renvoie `None` → casse la séquence typée `fills` avec
  `expected <class 'openpyxl.styles.fills.Fill'>`). Substitue un `PatternFill` par défaut.
- `Font.family.max = 99` — relâche le plafond openpyxl `NestedMinMax(min=0, max=14)` sur
  la police : des exports écrivent `<font><family val="34"/>` (Excel l'ignore) → openpyxl
  jette `ValueError: Max value is 14`, remonté en « could not read stylesheet / invalid XML ».
- `_safe_rgb_set` — normalise les couleurs `rgb` malformées : openpyxl exige du 8-hex aRGB
  (il ne pad que le cas 6-hex) et jette `ValueError: Colors must be aRGB hex values` sinon.
  Des exports (notamment nos xlsx `AI_GENERATED`) écrivent `rgb="FFFE9"` (5 hex) → coerce
  vers un 8-hex valide. ⚠️ **Cause racine côté source** : notre générateur Excel produit ces
  couleurs invalides — à corriger aussi en amont (émettre du 8-hex ARGB).

Ces patchs ne touchent que des **métadonnées de style** (remplissage, police), jamais les
**valeurs de cellules** (lues par `pd.read_excel`) → aucun impact sur le texte extrait des
fichiers sains ; ils ne se déclenchent que sur les fichiers malformés.

Autre fix connexe (pas un monkeypatch openpyxl) : `/process_file` écrit le fichier temp
avec le **suffixe d'extension** dérivé de `file.filename` (`NamedTemporaryFile(suffix=ext)`).
Sans ça, MarkItDown devine le type au contenu et **misroute** les Excel valides
(xlsx → PptxConverter, ou « no converter attempted » pour `.xls`/`.xlsx`).

➡️ Tout nouveau fix « fichier X malformé qui plante la conversion » suit ce même
pattern : monkeypatch openpyxl (ou le converter MarkItDown) en tête de `app.py`, avant
l'import markitdown. Toujours reproduire le crash sur un fichier réel avant/après.

⚠️ **Couplage version openpyxl** — les monkeypatches (`Fill.from_tree.__func__`,
`Font.family.max`) touchent des internes d'openpyxl (3.1.5, épinglé transitivement par
`markitdown==0.1.5`). Un bump openpyxl/markitdown peut casser ces lignes → le service
**throw à l'import** (fail-fast, pas de corruption silencieuse). Après toute montée de
version : re-valider les 3 patchs sur des fichiers réels. Pas de CI de tests dans ce repo →
valider en **canary** (1 fichier par catégorie) avant toute campagne de re-processing.

## Déploiement — Coolify build le Dockerfile depuis le repo

**Il n'y a PAS de push d'image à faire.** Coolify (Build Pack = `Dockerfile`) clone ce
repo et build le `dockerfile` lui-même. Le déploiement = **un push sur `main`**.

| Env | App Coolify | Serveur | Repo / branche | Accès |
| --- | --- | --- | --- | --- |
| **Prod** | `Markitdown API - Notae Build` (`fss8ck8c08woo0cok84ogggk`) | Notae Prod Coolify | `notae-ai/markitdown-api` @ `main`, `/dockerfile` | pas de FQDN public ; alias réseau interne `notae-apps.markitdown-notae-build:8500` |
| **Staging** | `MarkitDown - Notae` (`kw2lcsnubjvqgfv5zbnkccn8`) | Notae Staging Coolify | idem | `https://kw2lcsnubjvqgfv5zbnkccn8.staging.notae.ai` |

- Port exposé : **8500** (`EXPOSE 8500`, `uvicorn --port ${PORT:-8500}`).
- ⚠️ **Apps fantômes à ignorer** : plusieurs vieilles entrées Coolify pointent vers
  `coollabsio/coolify` (placeholders `Markitdown API - DEPRECATED`, `Markitdown`,
  `Markitdown API`, dont l'URL legacy `markitdown.staging.notae.ai:8490`). Ce ne sont
  **pas** le service buildé depuis ce repo. Vérifier le `git_repository` = 
  `notae-ai/markitdown-api` avant d'agir sur une app.

### Procédure

1. Modifier `app.py` (ou autre), tester en local (`./rebuild.sh` ou `uvicorn app:app`).
2. `git commit` + `git push origin main` (on push direct sur `main`).
3. **Redeploy Coolify** : si l'auto-deploy git est activé sur l'app, le push suffit ;
   sinon cliquer **Redeploy** sur l'app Coolify concernée (Prod et/ou Staging). Coolify
   rebuild le Dockerfile et relance le conteneur.
4. Vérifier le déploiement dans l'onglet **Deployments/Logs** de Coolify, puis tester
   `POST /process_file` (health check HTTP interne, désactivé par défaut sur l'app).

### Après un fix de conversion

Les fichiers déjà passés en `status=ERROR` côté Notae **ne se rejouent pas seuls** : il
faut re-déclencher leur traitement (`inngest.send({ name: "file/process", data: { fileId } })`)
une fois le service redéployé.

## Scripts upstream à ne PAS utiliser pour le déploiement Notae

- `push_to_ghcr.sh` — build + push vers `ghcr.io/dezoito/...` (workflow **upstream**,
  hors Notae). Inutile ici : Coolify build depuis le repo.
- `rebuild.sh` — pratique pour le **dev local** (rebuild + run le conteneur sur `:8490`),
  pas un chemin de déploiement.
