# Contributing to URLForge

Thank you for your interest in contributing to URLForge! We welcome contributions from developers worldwide to keep the system robust, high-performance, and compliant with modern distributed architecture standards.

## Code of Conduct

Please be respectful, collaborative, and constructive across all issue discussions and pull requests.

## How to Contribute

1. **Fork the Repository**: Create your own copy of the repo on GitHub.
2. **Clone and Branch**:
   ```bash
   git clone https://github.com/<your-username>/urlforge.git
   cd urlforge
   git checkout -b feature/your-feature-name
   ```
3. **Set Up Local Environment**:
   - Ensure you have Docker and Docker Compose installed.
   - Start the dependencies:
     ```bash
     make up
     ```
4. **Follow Engineering Guidelines**:
   - Write clean, idiomatic Go conforming to standard formatting (`gofmt`).
   - Maintain unit tests for all business logic, algorithms, and handlers.
   - Ensure all tests pass with zero race conditions:
     ```bash
     make test
     ```
5. **Commit Messages**:
   - Use conventional commit formats:
     - `feat: add geoip country fallback lookup`
     - `fix: prevent sequence overflow in snowflake generator`
     - `docs: update k6 benchmark results in README`
6. **Submit a Pull Request**:
   - Open a PR against `main`. Provide a clear summary of changes, benchmark impacts (if applicable), and testing steps.
