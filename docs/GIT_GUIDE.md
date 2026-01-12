# 🔧 Guide Git - LaCriee Project

**Date:** 2026-01-12
**Repository:** LaCriee Seafood Parser ELT Pipeline

---

## 📊 Historique du Repository

### Réinitialisation - 2026-01-12

Le repository a été réinitialisé pour avoir un historique propre et clair:

**Ancien état:**
- Historique confus dû à des copies de dossiers
- Commits désorganisés
- Structure non claire

**Nouveau départ:**
- ✅ Historique propre depuis v1.0
- ✅ Structure organisée (tests/, docs/, services/)
- ✅ Documentation complète
- ✅ .gitattributes pour line endings cohérents

### Commits Actuels

```bash
git log --oneline
# a68c6cf Add .gitattributes for consistent line endings
# 2a8b60a Initial commit - LaCriee ELT Pipeline v1.0
```

---

## 🌿 Stratégie de Branches

### Branches Principales

```
main (production)
  └─ Toujours stable et déployable
  └─ Tests passent à 100%
  └─ Documentation à jour
```

### Workflow Recommandé

```
main
  └── feature/new-vendor-parser      # Nouvelle fonctionnalité
  └── fix/vvqm-date-parsing          # Correction de bug
  └── docs/update-architecture       # Mise à jour documentation
  └── test/add-integration-tests     # Ajout de tests
```

---

## 📝 Convention de Commits

### Format Standard

```
<type>(<scope>): <description courte>

<corps optionnel expliquant le contexte>

<footer optionnel avec références>
```

### Types de Commits

| Type | Description | Exemple |
|------|-------------|---------|
| `feat` | Nouvelle fonctionnalité | `feat(parsers): add Hennequin PDF parser` |
| `fix` | Correction de bug | `fix(vvqm): correct date regex pattern` |
| `docs` | Documentation uniquement | `docs(readme): update installation steps` |
| `test` | Ajout/modification de tests | `test(demarne): add Excel edge cases` |
| `refactor` | Refactoring sans changement fonctionnel | `refactor(services): extract BigQuery logic` |
| `perf` | Amélioration de performance | `perf(parser): optimize PDF text extraction` |
| `chore` | Tâches de maintenance | `chore(deps): update requirements.txt` |
| `ci` | CI/CD | `ci(github): add automated testing workflow` |

### Exemples de Bons Commits

```bash
# Feature
git commit -m "feat(parsers): add Hennequin parser with PDF extraction

- Implement extract_hennequin_data_from_pdf()
- Add date extraction logic
- Add test file Samples/Hennequin/sample.pdf
- Update test_all_samples.py

Resolves #12"

# Bug fix
git commit -m "fix(demarne): handle empty Excel cells correctly

Previously crashed on NaN values in Code_Provider column.
Now uses fillna('') before processing.

Fixes #15"

# Documentation
git commit -m "docs(testing): add troubleshooting section

Added common error scenarios and solutions:
- Streaming buffer delays
- Module import errors
- BigQuery credential issues"
```

---

## 🔄 Workflow de Développement

### 1. Créer une Branche

```bash
# Pour une nouvelle fonctionnalité
git checkout -b feature/add-hennequin-parser

# Pour un bug fix
git checkout -b fix/streaming-buffer-timeout

# Pour de la documentation
git checkout -b docs/add-api-examples
```

### 2. Développer et Tester

```bash
# Faire vos modifications
# Tester localement
docker exec fastapi-pdf-parser python tests/test_all_samples.py

# Vérifier le statut
git status

# Voir les changements
git diff
```

### 3. Commit

```bash
# Ajouter les fichiers modifiés
git add services/import_service.py
git add tests/test_hennequin.py

# Commit avec message descriptif
git commit -m "feat(parsers): add Hennequin parser support

- Implement PDF parsing logic
- Add comprehensive tests
- Update documentation"
```

### 4. Pousser et Merge

```bash
# Pousser la branche
git push origin feature/add-hennequin-parser

# Créer une Pull Request sur GitHub/GitLab
# Après review et validation, merger dans main
```

---

## 📦 Commandes Git Utiles

### État et Historique

```bash
# Voir l'état actuel
git status

# Voir l'historique
git log --oneline --graph --all

# Voir les différences
git diff
git diff --staged

# Voir les fichiers modifiés
git diff --name-only
```

### Branches

```bash
# Lister les branches
git branch -a

# Créer une nouvelle branche
git checkout -b feature/my-feature

# Changer de branche
git checkout main

# Supprimer une branche locale
git branch -d feature/my-feature

# Supprimer une branche remote
git push origin --delete feature/my-feature
```

### Annuler des Changements

```bash
# Annuler un fichier non stagé
git restore file.py

# Annuler tous les fichiers non stagés
git restore .

# Unstage un fichier
git restore --staged file.py

# Annuler le dernier commit (garde les changements)
git reset --soft HEAD~1

# Annuler le dernier commit (supprime les changements)
git reset --hard HEAD~1
```

### Stash (Sauvegarder temporairement)

```bash
# Sauvegarder les changements en cours
git stash

# Lister les stash
git stash list

# Restaurer le dernier stash
git stash pop

# Appliquer un stash spécifique
git stash apply stash@{0}
```

---

## 🏷️ Tags et Versions

### Créer un Tag

```bash
# Tag léger
git tag v1.0.0

# Tag annoté (recommandé)
git tag -a v1.0.0 -m "LaCriee ELT Pipeline v1.0.0

Production release:
- 3/4 vendors operational
- Complete test suite
- Full documentation"

# Pousser les tags
git push origin --tags
```

### Lister les Tags

```bash
# Voir tous les tags
git tag

# Voir les détails d'un tag
git show v1.0.0

# Checkout un tag
git checkout v1.0.0
```

---

## 🚫 Fichiers Ignorés

Le `.gitignore` exclut automatiquement:

```
# Python
__pycache__/
*.pyc
*.pyo
venv/

# Credentials
credentials.json
*.pem
*.key

# Logs
*.log
logs/

# Environment
.env
.env.local

# IDE
.vscode/
.idea/

# OS
.DS_Store
Thumbs.db
nul

# Test artifacts
.pytest_cache/
.coverage

# BigQuery temp
*.bqschema
*.bqjob
```

**Note:** Les fichiers `Samples/` (PDFs, Excel) SONT versionnés car ils sont essentiels pour les tests.

---

## 🔍 Vérifications Avant Commit

### Checklist

- [ ] Code testé localement
```bash
docker exec fastapi-pdf-parser python tests/test_all_samples.py
```

- [ ] Pas de secrets dans le code
```bash
git diff | grep -i "password\|secret\|key\|token"
```

- [ ] Documentation à jour (si nécessaire)

- [ ] Message de commit descriptif

- [ ] .gitignore inclut les fichiers sensibles

### Script de Validation (Optionnel)

```bash
#!/bin/bash
# pre-commit-check.sh

echo "🔍 Running pre-commit checks..."

# Test Python syntax
echo "Checking Python syntax..."
python -m py_compile main.py services/*.py tests/*.py

# Run tests
echo "Running tests..."
docker exec fastapi-pdf-parser python tests/test_all_samples.py

# Check for secrets
echo "Checking for secrets..."
if git diff --cached | grep -iE "password|secret|key.*=|token"; then
    echo "⚠️  Warning: Possible secret detected!"
    exit 1
fi

echo "✅ All checks passed!"
```

---

## 📊 État Actuel du Repository

### Structure

```
main branch (stable)
├── 36 files tracked
├── 7981 lines of code
└── 2 commits (clean history)
```

### Fichiers Trackés

- ✅ Code source (main.py, services/, parsers/)
- ✅ Tests (tests/)
- ✅ Documentation (docs/, README.md, TESTING.md)
- ✅ Configuration (docker-compose.yml, requirements.txt)
- ✅ Scripts SQL (scripts/)
- ✅ Samples (PDF/Excel test files)

### Fichiers Ignorés

- ❌ Credentials (credentials.json)
- ❌ Virtual environments (venv/)
- ❌ Python cache (__pycache__/)
- ❌ Logs (*.log)
- ❌ Environment variables (.env)

---

## 🚀 Déploiement avec Git

### Tag de Version

Avant chaque déploiement production:

```bash
# Créer un tag de version
git tag -a v1.1.0 -m "Release v1.1.0 - Add Hennequin parser"

# Pousser le tag
git push origin v1.1.0

# Déployer la version taguée
git checkout v1.1.0
docker-compose up -d --build
```

### Rollback

Si problème en production:

```bash
# Revenir à la version précédente
git checkout v1.0.0
docker-compose up -d --build

# Ou créer une branche de hotfix
git checkout -b hotfix/critical-bug v1.0.0
# Fix le bug
git commit -m "hotfix: critical production bug"
git tag -a v1.0.1 -m "Hotfix v1.0.1"
```

---

## 📚 Ressources

- [Git Documentation Officielle](https://git-scm.com/doc)
- [Conventional Commits](https://www.conventionalcommits.org/)
- [Git Flow](https://nvie.com/posts/a-successful-git-branching-model/)

---

**Dernière Mise à Jour:** 2026-01-12
**Maintenu par:** LaCriee Development Team
