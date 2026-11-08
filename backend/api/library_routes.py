from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.workflow_library import LibraryConflict, WorkflowLibrary

router = APIRouter(prefix="/library", tags=["library"])
Kind = Literal["workflows", "subgraphs"]


class SaveDocument(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    folder: str = Field(default="", max_length=320)
    document: dict
    revision: int | None = Field(default=None, ge=1)


def library_call(operation):
    try:
        return operation(WorkflowLibrary())
    except FileNotFoundError:
        raise HTTPException(404, "Workflow not found.") from None
    except LibraryConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/{kind}")
def list_documents(kind: Kind):
    return library_call(lambda library: library.list(kind))


@router.get("/{kind}/{record_id}")
def get_document(kind: Kind, record_id: str):
    return library_call(lambda library: library.read(kind, record_id))


@router.post("/{kind}", status_code=201)
def create_document(kind: Kind, payload: SaveDocument):
    return library_call(lambda library: library.save(
        kind, payload.name, payload.folder, payload.document))


@router.put("/{kind}/{record_id}")
def update_document(kind: Kind, record_id: str, payload: SaveDocument):
    if payload.revision is None:
        raise HTTPException(400, "Updating a workflow requires its revision.")
    return library_call(lambda library: library.save(
        kind, payload.name, payload.folder, payload.document, record_id, payload.revision))


@router.delete("/{kind}/{record_id}")
def delete_document(kind: Kind, record_id: str, revision: int):
    library_call(lambda library: library.delete(kind, record_id, revision))
    return {"deleted": True}
