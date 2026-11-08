from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api.library_routes import router
from services.workflow_library import LibraryConflict, WorkflowLibrary


def document():
    return {"workflowId": "test-workflow", "nodes": [], "edges": []}


def test_round_trip_copy_revision_and_delete_preserve_other_user_files(tmp_path):
    library = WorkflowLibrary(tmp_path / "user")
    first = library.save("workflows", "实验/一", "细胞", document())
    second = library.save("workflows", "实验二", "", document())
    assert library.read("workflows", first["id"]) == first
    assert set(p.name for p in library.root.iterdir()) == {"workflows", "subgraphs", "custom_nodes"}
    assert library.path("workflows", first["id"]).name == first["id"] + ".json"
    updated = library.save("workflows", "新名称", "", document(), first["id"], 1)
    assert updated["revision"] == 2
    with pytest.raises(LibraryConflict):
        library.save("workflows", "旧版本", "", document(), first["id"], 1)
    with pytest.raises(LibraryConflict):
        library.delete("workflows", first["id"], 1)
    library.delete("workflows", first["id"], 2)
    assert library.read("workflows", second["id"]) == second


def test_concurrent_updates_allow_only_one_writer(tmp_path):
    library = WorkflowLibrary(tmp_path)
    record = library.save("workflows", "one", "", document())
    def update(i):
        try:
            return library.save("workflows", f"name-{i}", "", document(), record["id"], 1)["revision"]
        except LibraryConflict:
            return "conflict"
    with ThreadPoolExecutor(max_workers=2) as executor:
        assert sorted(executor.map(update, range(2)), key=str) == [2, "conflict"]
    assert library.read("workflows", record["id"])["revision"] == 2


def test_bad_files_are_reported_and_paths_cannot_escape(tmp_path):
    library = WorkflowLibrary(tmp_path / "user")
    library.directory("workflows").joinpath("broken.json").write_text("{", encoding="utf8")
    assert len(library.list("workflows")["warnings"]) == 1
    with pytest.raises(ValueError):
        library.read("workflows", "../../outside")
    with pytest.raises(ValueError):
        library.directory("../elsewhere")
    with pytest.raises(ValueError):
        library.save("workflows", " ", "", document())
    with pytest.raises(ValueError):
        library.save("workflows", "oversize", "", {**document(), "padding": "a" * (8 * 1024 * 1024)})
    assert not list(tmp_path.glob("*.json"))


def test_http_crud_and_conflict_without_loading_algorithm_plugins(tmp_path, monkeypatch):
    monkeypatch.setenv("WorkFlow_USER_DIR", str(tmp_path / "user"))
    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as client:
        payload = {"name": "one", "document": document()}
        created = client.post("/library/workflows", json=payload)
        assert created.status_code == 201
        record = created.json()
        url = "/library/workflows/" + record["id"]
        assert client.get(url).json() == record
        assert "document" not in client.get("/library/workflows").json()["items"][0]
        assert client.put(url, json=payload).status_code == 400
        assert client.put(url, json={**payload, "revision": 1}).json()["revision"] == 2
        assert client.put(url, json={**payload, "revision": 1}).status_code == 409
        assert client.delete(url + "?revision=1").status_code == 409
        assert client.delete(url + "?revision=2").status_code == 200
        assert client.get(url).status_code == 404
        assert client.get("/library/arbitrary").status_code == 422
        assert client.post("/library/subgraphs", json=payload).status_code == 201
    assert len(list((tmp_path / "user" / "subgraphs").glob("*.json"))) == 1


def test_missing_plugin_is_still_a_portable_saved_document(tmp_path):
    library = WorkflowLibrary(tmp_path)
    portable = {**document(), "nodes": [{"id": "external", "data": {"opType": "NotInstalledHere"}}]}
    record = library.save("workflows", "foreign", "", portable)
    assert json.loads(library.path("workflows", record["id"]).read_text(encoding="utf8"))["document"] == portable
