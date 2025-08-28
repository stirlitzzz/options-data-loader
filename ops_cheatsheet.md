# Ops Cheat Sheet (DuckDB + Git + venv + pyproject)

## 0) New machine quickstart
python -m venv .venv
. .venv/bin/activate
pip install -e .                    # reads pyproject.toml, installs CLIs
set -a; [ -f .env ] && source .env; set +a  # load DATA_DIR/API keys

## 1) Update repo (after pushing from laptop)
git fetch --all --prune
git pull --rebase origin main
. .venv/bin/activate && pip install -e .    # only if pyproject/deps changed

## 2) Commit with pre-commit (Black/Ruff auto-fix)
pre-commit run --all-files
git add -A
git commit -m "msg"
# if hooks modify again, repeat add+commit

## 3) venv basics
python -m venv .venv
. .venv/bin/activate      # enter
deactivate                # exit
which python; which pip   # sanity

## 4) .env into the shell (so $IVOL_DATA_DIR etc. work)
set -a; source .env; set +a
# one-off:
dotenv -f .env run -- <cmd>

## 5) pyproject edits (new CLI or dep)
# add under [project.scripts] or dependencies, then:
. .venv/bin/activate
pip install -e .

## 6) tmux (don’t background loops with Ctrl-Z)
tmux new -s work          # start
# Ctrl-b d to detach
tmux ls                   # list
tmux attach -t work       # reattach
tmux new -d -s job "<command>"  # start detached with a command

## 7) DuckDB patterns
# query a folder of parquet:
duckdb -c "SELECT COUNT(*) FROM read_parquet('$IVOL_DATA_DIR/raw/*.parquet');"

# run a parametrized .sql with env vars:
DATA_DIR="$IVOL_DATA_DIR" START_DATE=2006-01-01 END_DATE=2006-12-31 \
envsubst < sql/01_pairs.sql | duckdb

## 8) Git rescue one-liners
# keep my local edits but update:
git stash -u && git pull --rebase origin main && git stash pop

# throw away local edits and match remote:
git fetch origin && git reset --hard origin/main && git clean -fd

# switched to HTTPS if SSH nags for key:
git remote set-url origin https://github.com/USER/REPO.git

## 9) “did my year loop iterate?”
grep -h ">>> YEAR" "$IVOL_DATA_DIR"/logs/run_*.log | tail
ls -1 "$IVOL_DATA_DIR/raw" | wc -l

## 10) disk sanity
du -sh "$IVOL_DATA_DIR/raw"
df -h "$IVOL_DATA_DIR"
df -i "$IVOL_DATA_DIR"   # inodes if many small files
