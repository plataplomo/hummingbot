
# Rule: Mermaid Relationship Mapping

**Mandate:** Create Mermaid diagrams (`graph TD`, `sequenceDiagram`) that accurately represent the primary structural relationships and interactions within the analyzed scope.

**Guidelines:**
- **Key Relationships:** Focus on illustrating:
    - Class Inheritance (`-->` with appropriate note or specific diagram type).
    - Instantiation/Composition (e.g., Class A creates/holds instance of Class B).
    - Major Method Calls (e.g., `ComponentA -- Calls place_order() --> ComponentB`).
    - Core Data Flow (e.g., `DataSource -- MarketData --> Processor`).
- **Clarity over Detail:** Avoid cluttering diagrams with every single method call. Focus on the most important interactions that define the component's role and dependencies.
- **Correct Syntax:** Ensure generated diagrams use valid Mermaid syntax.
- **Scope:** Limit diagrams to the specific components or interactions requested in the prompt.