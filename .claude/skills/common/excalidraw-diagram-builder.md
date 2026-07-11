# Skill: Excalidraw Diagram Builder

## Metadata
- **ID**: `common/excalidraw-diagram-builder`
- **Category**: common
- **Agents**: All agents
- **Dependencies**: Read, Write

## Description

Generate architecture, data flow, and system diagrams as `.excalidraw` files. All project diagrams use the Excalidraw format for visual consistency and editability in VS Code (via Excalidraw extension) or excalidraw.com.

## When to Use

- Architecture proposals and technical documents need visual diagrams
- Data flow diagrams for CDC pipelines, streaming, and integrations
- System topology and network diagrams
- Any diagram referenced in markdown documentation

## Inputs

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| diagram_type | string | Yes | Type: `architecture`, `data-flow`, `network`, `erd`, `sequence` |
| description | string | Yes | What the diagram should represent |
| output_path | string | Yes | File path for the `.excalidraw` file |
| style | string | No | `formal` (roughness=0) or `sketch` (roughness=1, default) |

## Outputs

| Output | Type | Description |
|--------|------|-------------|
| excalidraw_file | file | Valid `.excalidraw` JSON file |
| md_reference | string | Markdown image reference for documentation |

---

## Excalidraw File Structure

Every `.excalidraw` file follows this JSON structure:

```json
{
  "type": "excalidraw",
  "version": 2,
  "source": "https://excalidraw.com",
  "elements": [],
  "appState": {
    "gridSize": 20,
    "viewBackgroundColor": "#ffffff"
  },
  "files": {}
}
```

---

## Element Templates

### Shape (Rectangle, Ellipse, Diamond)

```json
{
  "type": "rectangle",
  "id": "<unique-id>",
  "x": 0,
  "y": 0,
  "width": 200,
  "height": 80,
  "angle": 0,
  "strokeColor": "#1971c2",
  "backgroundColor": "#a5d8ff",
  "fillStyle": "solid",
  "strokeWidth": 2,
  "strokeStyle": "solid",
  "roughness": 1,
  "opacity": 100,
  "groupIds": [],
  "boundElements": [
    {"id": "<text-id>", "type": "text"},
    {"id": "<arrow-id>", "type": "arrow"}
  ],
  "roundness": {"type": 3},
  "seed": 1000000,
  "version": 1,
  "versionNonce": 2000000,
  "isDeleted": false,
  "locked": false
}
```

### Text (Bound to Shape)

```json
{
  "type": "text",
  "id": "<text-id>",
  "x": 0,
  "y": 0,
  "width": 100,
  "height": 25,
  "angle": 0,
  "strokeColor": "#1e1e1e",
  "backgroundColor": "transparent",
  "fillStyle": "solid",
  "strokeWidth": 2,
  "strokeStyle": "solid",
  "roughness": 0,
  "opacity": 100,
  "groupIds": [],
  "boundElements": null,
  "seed": 3000000,
  "version": 1,
  "versionNonce": 4000000,
  "isDeleted": false,
  "locked": false,
  "text": "Label",
  "fontSize": 16,
  "fontFamily": 2,
  "textAlign": "center",
  "verticalAlign": "middle",
  "containerId": "<shape-id>",
  "originalText": "Label",
  "autoResize": true,
  "lineHeight": 1.25
}
```

### Arrow (Connecting Shapes)

```json
{
  "type": "arrow",
  "id": "<arrow-id>",
  "x": 0,
  "y": 0,
  "width": 100,
  "height": 0,
  "angle": 0,
  "strokeColor": "#1e1e1e",
  "backgroundColor": "transparent",
  "fillStyle": "solid",
  "strokeWidth": 2,
  "strokeStyle": "solid",
  "roughness": 1,
  "opacity": 100,
  "groupIds": [],
  "boundElements": null,
  "seed": 5000000,
  "version": 1,
  "versionNonce": 6000000,
  "isDeleted": false,
  "locked": false,
  "points": [[0, 0], [100, 0]],
  "lastCommittedPoint": null,
  "startBinding": {
    "elementId": "<source-shape-id>",
    "focus": 0,
    "gap": 5
  },
  "endBinding": {
    "elementId": "<target-shape-id>",
    "focus": 0,
    "gap": 5
  },
  "startArrowhead": null,
  "endArrowhead": "arrow"
}
```

### Standalone Text (Labels, Titles)

```json
{
  "type": "text",
  "id": "<text-id>",
  "x": 0,
  "y": 0,
  "width": 200,
  "height": 30,
  "angle": 0,
  "strokeColor": "#1e1e1e",
  "backgroundColor": "transparent",
  "fillStyle": "solid",
  "strokeWidth": 2,
  "strokeStyle": "solid",
  "roughness": 0,
  "opacity": 100,
  "groupIds": [],
  "boundElements": null,
  "seed": 7000000,
  "version": 1,
  "versionNonce": 8000000,
  "isDeleted": false,
  "locked": false,
  "text": "Title Text",
  "fontSize": 28,
  "fontFamily": 2,
  "textAlign": "left",
  "verticalAlign": "top",
  "containerId": null,
  "originalText": "Title Text",
  "autoResize": true,
  "lineHeight": 1.25
}
```

---

## Color Palette (Project Standard)

### Component Colors

| Component Type | Background | Stroke | Usage |
|---------------|-----------|--------|-------|
| **Source / Database** | `#a5d8ff` | `#1971c2` | Postgres, databases, data sources |
| **Streaming / Kafka** | `#99e9f2` | `#0c8599` | Kafka topics, Confluent Cloud |
| **Processing** | `#d0bfff` | `#7048e8` | Flink SQL, transformations |
| **Sink / Destination** | `#b2f2bb` | `#2f9e44` | Snowflake, S3, outputs |
| **Warning / Decision** | `#ffec99` | `#f08c00` | Decisions, warnings, config |
| **Critical / Error** | `#ffc9c9` | `#e03131` | DLQ, errors, alerts |
| **Neutral / Infrastructure** | `#e9ecef` | `#495057` | Network, infra, generic |
| **Governance / Security** | `#eebefa` | `#9c36b5` | Schema Registry, RBAC, compliance |

### Arrow Colors

| Flow Type | Color | Style |
|-----------|-------|-------|
| **Data flow** | `#1e1e1e` | `solid` |
| **CDC stream** | `#1971c2` | `solid` |
| **Async / Event** | `#495057` | `dashed` |
| **Error path** | `#e03131` | `dotted` |

---

## Layout Guidelines

### Spacing

| Between | Pixels |
|---------|--------|
| Shapes (horizontal) | 80-120 |
| Shapes (vertical) | 60-100 |
| Groups / Sections | 150-200 |
| Title to first element | 60 |

### Standard Sizes

| Element | Width | Height |
|---------|-------|--------|
| Component box | 180-220 | 70-90 |
| Small label box | 120-150 | 50-60 |
| Section header | 400-600 | 40 |
| Title | 500+ | 40 |

### Flow Direction

| Diagram Type | Direction |
|-------------|-----------|
| Architecture | Left → Right |
| Data pipeline | Left → Right |
| Network topology | Top → Bottom |
| Hierarchy | Top → Bottom |

---

## Binding Rules

### Text ↔ Shape

1. Shape `boundElements` array must include `{"id": "<text-id>", "type": "text"}`
2. Text `containerId` must reference the shape's `id`
3. Text `textAlign` should be `"center"` and `verticalAlign` should be `"middle"`

### Arrow ↔ Shape

1. Source shape `boundElements` array must include `{"id": "<arrow-id>", "type": "arrow"}`
2. Target shape `boundElements` array must include `{"id": "<arrow-id>", "type": "arrow"}`
3. Arrow `startBinding.elementId` must reference source shape `id`
4. Arrow `endBinding.elementId` must reference target shape `id`
5. Arrow `points[0]` must always be `[0, 0]`

---

## Execution Steps

1. **Analyze** the description and identify all components
2. **Plan layout** — determine positions, groupings, and flow direction
3. **Generate elements** — create shapes, text labels, and arrows with unique IDs
4. **Validate bindings** — ensure all boundElements, containerId, startBinding, endBinding are consistent
5. **Write file** — output valid JSON as `.excalidraw`
6. **Return MD reference** — provide markdown link for documentation

### ID Convention

Use descriptive, unique IDs:
- Shapes: `rect-<component-name>` (e.g., `rect-postgres`, `rect-kafka`, `rect-iceberg`)
- Text: `text-<component-name>` (e.g., `text-postgres`, `text-kafka`)
- Arrows: `arrow-<source>-to-<target>` (e.g., `arrow-postgres-to-kafka`)
- Groups: `group-<section>` (e.g., `group-onprem`, `group-cloud`)
- Standalone text: `label-<description>` (e.g., `label-title`, `label-section-aws`)

### Seed Convention

Use incrementing seeds for consistent rendering:
- Start at `1000000` and increment by `1` per element
- `versionNonce` starts at `2000000` and increments by `1`

---

## Markdown Integration

Reference `.excalidraw` diagrams in markdown using relative paths:

```markdown
## Arquitetura de Ingestão

![Arquitetura CDC Postgres → Confluent Cloud → Iceberg](./diagramas/arquitetura-ingestao-cdc.excalidraw.png)

> Diagrama editável: [arquitetura-ingestao-cdc.excalidraw](./diagramas/arquitetura-ingestao-cdc.excalidraw)
```

For VS Code with Excalidraw extension, `.excalidraw` files render inline. For GitHub/other renderers, export a `.png` alongside.

---

## Example: CDC Pipeline Diagram

A CDC pipeline diagram for Postgres → Confluent Cloud → Tableflow/Iceberg would include:

| Layer | Components | Color |
|-------|-----------|-------|
| Sources (left) | Postgres source DB box | Blue (`#a5d8ff`) |
| Network | Direct Connect arrow | Gray, dashed |
| Streaming (center) | Confluent Cloud group with Kafka Topics, Schema Registry | Cyan (`#99e9f2`) |
| Processing | Flink SQL | Purple (`#d0bfff`) |
| Sinks (right) | Tableflow → Iceberg (Glue/Snowflake Open Catalog/Unity) | Green (`#b2f2bb`) |
| Governance | Schema Registry, Data Contracts | Purple (`#eebefa`) |
| Error handling | DLQ topic | Red (`#ffc9c9`) |

---

## Validation Checklist

Before writing the `.excalidraw` file:

- [ ] Valid JSON (no trailing commas, proper quoting)
- [ ] All elements have unique `id` values
- [ ] All text bound to shapes have matching `containerId` ↔ `boundElements`
- [ ] All arrows have matching `startBinding`/`endBinding` ↔ `boundElements` on shapes
- [ ] Arrow `points[0]` is `[0, 0]`
- [ ] No overlapping elements (check x, y, width, height)
- [ ] Consistent color palette per component type
- [ ] `fontFamily` is `2` (Helvetica) for formal diagrams
- [ ] `roughness` matches requested style (0=formal, 1=sketch)

---

**Version**: 1.0
**Last Updated**: 2026-02-16
