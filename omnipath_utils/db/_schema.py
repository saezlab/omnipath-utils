"""SQLAlchemy 2.0 ORM models for the omnipath_utils database."""

from __future__ import annotations

from sqlalchemy import (
    Text,
    Float,
    Index,
    String,
    Integer,
    DateTime,
    BigInteger,
    ForeignKey,
    SmallInteger,
    func,
)
from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column


class Base(DeclarativeBase):
    pass


class IdType(Base):
    __tablename__ = 'id_type'
    __table_args__ = {'schema': 'omnipath_utils'}

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    label: Mapped[str | None] = mapped_column(String(128))
    entity_type: Mapped[str | None] = mapped_column(String(32))
    curie_prefix: Mapped[str | None] = mapped_column(String(32))
    url_pattern: Mapped[str | None] = mapped_column(String(256))
    id_pattern: Mapped[str | None] = mapped_column(String(128))


class Backend(Base):
    __tablename__ = 'backend'
    __table_args__ = {'schema': 'omnipath_utils'}

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)


class Organism(Base):
    __tablename__ = 'organism'
    __table_args__ = {'schema': 'omnipath_utils'}

    ncbi_tax_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    latin_name: Mapped[str | None] = mapped_column(String(128))
    common_name: Mapped[str | None] = mapped_column(String(64))
    short_latin: Mapped[str | None] = mapped_column(String(64))
    ensembl_name: Mapped[str | None] = mapped_column(String(64))
    kegg_code: Mapped[str | None] = mapped_column(String(8))
    mirbase_code: Mapped[str | None] = mapped_column(String(8))
    oma_code: Mapped[str | None] = mapped_column(String(8))
    uniprot_code: Mapped[str | None] = mapped_column(String(8))
    dbptm_code: Mapped[str | None] = mapped_column(String(16))


class IdMapping(Base):
    __tablename__ = 'id_mapping'
    __table_args__ = (
        Index('idx_mapping_lookup', 'source_type_id', 'target_type_id', 'ncbi_tax_id', 'source_id'),
        Index('idx_mapping_reverse', 'target_type_id', 'source_type_id', 'ncbi_tax_id', 'target_id'),
        Index('idx_mapping_table', 'source_type_id', 'target_type_id', 'ncbi_tax_id'),
        {'schema': 'omnipath_utils'},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_type_id: Mapped[int] = mapped_column(SmallInteger, ForeignKey('omnipath_utils.id_type.id'), nullable=False)
    target_type_id: Mapped[int] = mapped_column(SmallInteger, ForeignKey('omnipath_utils.id_type.id'), nullable=False)
    ncbi_tax_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_id: Mapped[str] = mapped_column(String(64), nullable=False)
    target_id: Mapped[str] = mapped_column(String(64), nullable=False)
    backend_id: Mapped[int] = mapped_column(SmallInteger, ForeignKey('omnipath_utils.backend.id'), nullable=False)


class Reflist(Base):
    __tablename__ = 'reflist'
    __table_args__ = (
        Index('idx_reflist_lookup', 'id_type_id', 'ncbi_tax_id', 'list_name'),
        Index('idx_reflist_contains', 'id_type_id', 'ncbi_tax_id', 'list_name', 'identifier'),
        {'schema': 'omnipath_utils'},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    identifier: Mapped[str] = mapped_column(String(64), nullable=False)
    id_type_id: Mapped[int] = mapped_column(SmallInteger, ForeignKey('omnipath_utils.id_type.id'), nullable=False)
    ncbi_tax_id: Mapped[int] = mapped_column(Integer, nullable=False)
    list_name: Mapped[str] = mapped_column(String(32), nullable=False)


class BuildInfo(Base):
    __tablename__ = 'build_info'
    __table_args__ = {'schema': 'omnipath_utils'}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(String(64), nullable=False)
    source_type: Mapped[str | None] = mapped_column(String(64))
    target_type: Mapped[str | None] = mapped_column(String(64))
    ncbi_tax_id: Mapped[int | None] = mapped_column(Integer)
    backend: Mapped[str | None] = mapped_column(String(32))
    row_count: Mapped[int | None] = mapped_column(BigInteger)
    built_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    duration_secs: Mapped[float | None] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default='pending')


class Orthology(Base):
    __tablename__ = 'orthology'
    __table_args__ = (
        Index('idx_orth_fwd', 'source_tax_id', 'target_tax_id', 'id_type', 'resource', 'source_id'),
        Index('idx_orth_rev', 'target_tax_id', 'source_tax_id', 'id_type', 'resource', 'target_id'),
        {'schema': 'omnipath_utils'},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(String(64), nullable=False)
    target_id: Mapped[str] = mapped_column(String(64), nullable=False)
    source_tax_id: Mapped[int] = mapped_column(Integer, nullable=False)
    target_tax_id: Mapped[int] = mapped_column(Integer, nullable=False)
    id_type: Mapped[str] = mapped_column(String(32), nullable=False)
    resource: Mapped[str] = mapped_column(String(32), nullable=False)
    rel_type: Mapped[str | None] = mapped_column(String(8))
    score: Mapped[float | None] = mapped_column(Float)
    confidence: Mapped[str | None] = mapped_column(String(8))
    orth_type: Mapped[str | None] = mapped_column(String(16))
    n_sources: Mapped[int | None] = mapped_column(Integer)
    support: Mapped[str | None] = mapped_column(Text)
