# Contributing to CHARMTwinsights
 
Thanks for your interest in CHARMTwinsights!
 
This project is part of the CHARM suite of tools and is under active development. Contributions are welcome, but please note that this repository primarily prioritizes project-driven work (e.g., funded milestones, internal roadmaps, and integration requirements). As a result, not all external contributions may be accepted.
 
## Quick Guidelines
 
Before starting work, please:
 
- Search existing Issues first (including closed issues).
- Keep pull requests to one feature or bugfix.
 
## How to Contribute
 
### 1) Report an Issue
 
Bug reports and suggestions for use cases are the most helpful contribution.
 
When filing an issue, please include:
- what you expected to happen
- what happened instead
- steps to reproduce
- OS + Docker version (if relevant)
- relevant logs or error output
 
### 2) Propose a Change
 
If you want to contribute code:
1. Open an Issue describing the change first (recommended for anything non-trivial)
2. Fork the repo
3. Create a feature branch:
 
   git checkout -b feature/my-change
 
4. Make your changes
5. Open a Pull Request
 
## Development Setup
 
Most development and testing is done through Docker / Docker Compose.
 
Please follow the instructions in `README.md` for building and starting the application.
 
In general, development workflows include:
 
 ```
# from app/
./build_all.sh
docker compose up --detach
```

See also:
- `DOCKER_TIPS.md` for troubleshooting

 
## Model Development Contributions
 
Model developers should start with the templates under `model-templates/`. While TWINSight does include
a small number of 'default' models, because it is designed to ingest and host externally-developed models, develop these in independent repositories instead. Use GitHub Issues or Discussions to alert the maintainers
if you've developed a model for use with TWINSight.
 
## Security
 
Please do **not** open public issues for sensitive vulnerabilities.
 
Instead, report concerns to the maintainers directly.
 
## License
 
By contributing, you agree that your contributions will be licensed under the repository’s license.
