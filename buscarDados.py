import os
import requests
import pandas as pd
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


GITHUB_TOKEN = ""

if not GITHUB_TOKEN:
    raise ValueError("Defina o token antes de rodar: set GITHUB_TOKEN=seu_token")

HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
URL = "https://api.github.com/graphql"

TARGET_VALID_REPOS = 200
MAX_PRS_PER_REPO = 20
DELAY = 6 
MAX_WORKERS = 3
OUTPUT_FILE = "prs_dataset.csv"

csv_lock = Lock()
all_data = []

REPOS_QUERY = """
query ($queryString: String!, $cursor: String) {
  search(query: $queryString, type: REPOSITORY, first: 10, after: $cursor) {
    pageInfo {
      endCursor
      hasNextPage
    }
    nodes {
      ... on Repository {
        nameWithOwner
        pullRequests(states: [MERGED, CLOSED]) {
          totalCount
        }
      }
    }
  }
}
"""

PRS_QUERY = """
query ($owner: String!, $name: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(
      first: 50
      after: $cursor
      states: [MERGED, CLOSED]
      orderBy: { field: UPDATED_AT, direction: DESC }
    ) {
      pageInfo {
        endCursor
        hasNextPage
      }
      nodes {
        createdAt
        mergedAt
        closedAt
        additions
        deletions
        changedFiles
        bodyText
        reviews {
          totalCount
        }
        comments {
          totalCount
        }
        participants {
          totalCount
        }
      }
    }
  }
}
"""

def load_existing_data():
    if os.path.exists(OUTPUT_FILE):
        print("📂 Carregando dados existentes...")
        df = pd.read_csv(OUTPUT_FILE)
        return df.to_dict("records")
    return []


def get_processed_repos():
    if not os.path.exists(OUTPUT_FILE):
        return set()

    df = pd.read_csv(OUTPUT_FILE)
    processed = set(df["repo"].unique())

    print(f"♻️ Repos já processados: {len(processed)}")
    return processed

def run_query(query, variables, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.post(
                URL,
                json={"query": query, "variables": variables},
                headers=HEADERS,
                timeout=30
            )

            if response.status_code == 403:
                print("🚫 Rate limit atingido, esperando 60s...")
                time.sleep(60)
                continue

            if response.text.strip() == "":
                print("⚠️ Resposta vazia da API")
                time.sleep(5)
                continue

            try:
                result = response.json()
            except Exception:
                print("❌ Resposta NÃO é JSON:")
                print(response.text[:300])
                time.sleep(15)
                continue

            if response.status_code != 200:
                print(f"❌ Erro HTTP {response.status_code}")
                print(result)
                time.sleep(5)
                continue

            if "errors" in result:
                print("❌ Erro GraphQL:")
                print(result["errors"])
                time.sleep(5)
                continue

            return result

        except Exception as e:  
            print(f"❌ Tentativa {attempt+1} falhou: {e}")
            time.sleep(5)

    print("❌ Falhou após várias tentativas")
    return None

def calculate_hours(start, end):
    try:
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        return (end_dt - start_dt).total_seconds() / 3600
    except Exception:
        return None


def is_valid_pr(pr):
    created = pr.get("createdAt")
    end_date = pr.get("mergedAt") or pr.get("closedAt")

    if not created or not end_date:
        return False

    review_count = pr.get("reviews", {}).get("totalCount", 0)

    if review_count < 1:
        return False

    hours = calculate_hours(created, end_date)

    if hours is None or hours <= 1:
        return False

    return True


def build_pr_data(repo_name, pr):
    end_date = pr.get("mergedAt") or pr.get("closedAt")
    hours = calculate_hours(pr.get("createdAt"), end_date)
    additions = pr.get("additions") or 0
    deletions = pr.get("deletions") or 0


    return {
    "repo": repo_name,
    "status": "MERGED" if pr.get("mergedAt") else "CLOSED",
    "files_changed": pr.get("changedFiles", 0),
    "additions": additions,
    "deletions": deletions,
    "lines_changed": additions + deletions,
    "description_length": len(pr.get("bodyText") or ""),
    "review_count": pr.get("reviews", {}).get("totalCount", 0),
    "comment_count": pr.get("comments", {}).get("totalCount", 0),
    "participants": pr.get("participants", {}).get("totalCount", 0),
    "analysis_time_hours": hours,
}


def collect_prs_from_repo(repo):
    repo_name = repo["nameWithOwner"]
    owner, name = repo_name.split("/", 1)

    repo_prs = []
    cursor = None

    print(f"🚀 Thread iniciada para {repo_name}")

    while len(repo_prs) < MAX_PRS_PER_REPO:
        variables = {
            "owner": owner,
            "name": name,
            "cursor": cursor
        }

        result = run_query(PRS_QUERY, variables)

        if not result:
            break

        pull_requests = result["data"]["repository"]["pullRequests"]
        prs = pull_requests["nodes"]

        for pr in prs:
            if is_valid_pr(pr):
                repo_prs.append(build_pr_data(repo_name, pr))

            if len(repo_prs) >= MAX_PRS_PER_REPO:
                break

        page_info = pull_requests["pageInfo"]

        if not page_info["hasNextPage"]:
            break

        cursor = page_info["endCursor"]
        time.sleep(DELAY)

    print(f"✅ {repo_name}: {len(repo_prs)} PRs válidos")

    return repo_prs


def save_dataset():
    with csv_lock:
        df = pd.DataFrame(all_data)
        df.to_csv(OUTPUT_FILE, index=False)
        print("💾 CSV atualizado")


def get_valid_repositories():
    valid_repositories = []
    cursor = None
    repo_checked = 0

    while len(valid_repositories) < TARGET_VALID_REPOS:
        print(f"\n🔎 Buscando repositórios... ({len(valid_repositories)}/{TARGET_VALID_REPOS})")

        variables = {
            "queryString": "stars:>1000 sort:stars-desc",
            "cursor": cursor
        }

        result = run_query(REPOS_QUERY, variables)

        if not result:
            continue

        search_data = result["data"]["search"]
        repos = search_data["nodes"]

        for repo in repos:
            repo_checked += 1
            repo_name = repo["nameWithOwner"]
            total_prs = repo["pullRequests"]["totalCount"]

            print(f"📦 Checando repo {repo_checked}: {repo_name}")

            if total_prs < 100:
                print(f"⏭️ Ignorado: apenas {total_prs} PRs MERGED/CLOSED")
                continue

            print(f"✅ Repo válido: {repo_name} ({total_prs} PRs)")
            valid_repositories.append(repo)

            if len(valid_repositories) >= TARGET_VALID_REPOS:
                break

        cursor = search_data["pageInfo"]["endCursor"]

        if not search_data["pageInfo"]["hasNextPage"]:
            break

        time.sleep(DELAY)

    return valid_repositories

def main():
    global all_data

    print("🚀 Iniciando coleta...\n")

    all_data = load_existing_data()
    processed_repos = get_processed_repos()

    repositories = get_valid_repositories()

    repositories = [
        repo for repo in repositories
        if repo["nameWithOwner"] not in processed_repos
    ]

    print(f"\n✅ Repositórios restantes: {len(repositories)}")
    print(f"🧵 Iniciando coleta com {MAX_WORKERS} threads...\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(collect_prs_from_repo, repo)
            for repo in repositories
        ]

        for future in as_completed(futures):
            try:
                repo_prs = future.result()

                with csv_lock:
                    all_data.extend(repo_prs)

                save_dataset()

            except Exception as e:
                print(f"❌ Erro em uma thread: {e}")

    print("\n🎉 Coleta finalizada!")
    print(f"Total de PRs coletados: {len(all_data)}")
    print(f"Arquivo gerado: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()