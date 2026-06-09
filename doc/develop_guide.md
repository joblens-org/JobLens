# Development Collaboration Guidelines

## 1. Branch Structure

### 1.1 Core Branches
- **main**: Production stable branch
  - Contains only released production code
  - Can only be merged via Pull Request (PR)
  - Protected branch, direct pushes are forbidden
  - Triggers GitLab CI/CD deployment to production

- **develop**: Integration branch
  - Daily development integration branch
  - All feature development is merged here upon completion
  - Triggers GitLab CI/CD deployment to dev/test environment

### 1.2 Temporary Branches
- **feature/***: Feature branches
  - Format: `feature/<feature-name>-<date>` or `feature/<issue-number>-<short-description>`
  - Example: `feature/user-auth-20231015` or `feature/#123-add-login`
  - Created from the `develop` branch
  - Merged back into `develop` after completion
  - **Does not trigger automatic deployment**

- **hotfix/***: Emergency fix branches
  - Format: `hotfix/<issue-description>-<date>`
  - Example: `hotfix/fix-crash-20231015`
  - Created from the `main` branch
  - Merged into both `main` and `develop` after the fix
  - **Does not trigger automatic deployment**

## 2. Development Workflow

### 2.1 Feature Development Workflow
```
1. Create a feature branch from develop
   git checkout develop
   git pull origin develop
   git checkout -b feature/your-feature-name

2. Develop on the feature branch
   # Do development work, commit multiple times

3. Push to remote repository
   git push origin feature/your-feature-name

4. Create a Pull Request (PR)
   - Target branch: develop
   - Add description and reviewers
   - Wait for CI checks to pass

5. Code review and merge
   - At least 1 reviewer must approve
   - Resolve conflicts (if any)
   - Merge into the develop branch
```

### 2.2 Hotfix Workflow
```
1. Create a hotfix branch from main
   git checkout main
   git pull origin main
   git checkout -b hotfix/issue-description

2. Fix and test
   # Fix the issue and test thoroughly

3. Create two Pull Requests
   PR1: hotfix → main
   PR2: hotfix → develop

4. Merge order
   - Merge into main first (deploy to production)
   - Then merge into develop (keep in sync)
```

## 3. Commit Conventions

### 3.1 Commit Message Format
```
type(scope): description

body (optional)

footer (optional)
```

### 3.2 Type Descriptions
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation updates
- `style`: Code formatting (does not affect functionality)
- `refactor`: Code refactoring
- `test`: Test-related
- `chore`: Build process or tooling changes

### 3.3 Examples
```
feat(auth): add user login functionality

- Implement JWT token authentication
- Add user login endpoint
- Update related documentation

Closes #123
```

## 4. Pull Request (PR) Guidelines

### 4.1 PR Title Format
```
[Type] Brief description
```
Example: `[Feature] Add user management system`

### 4.2 PR Description Template
```markdown
## Change Description
Briefly describe the changes in this PR

## Related Issues
Linked issue numbers: #123

## Test Instructions
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing steps...

## Checklist
- [ ] Code has been self-reviewed
- [ ] Documentation has been updated
- [ ] No compilation warnings
- [ ] Follows code style guidelines

## Screenshots (if applicable)
```

## 5. CI/CD Deployment

### 5.1 Automatic Trigger Rules
- **develop branch**: 
  - Automatically builds after each merge
  - Deploys to dev/test environment
  - Runs complete test suite

- **main branch**:
  - Automatically builds after each merge
  - Deploys to production environment
  - Runs production environment tests

### 5.2 Manual Deployment
To manually trigger deployment, operate on the GitLab CI/CD pipeline page.

## 6. C++ Development Notes

### 6.1 Code Standards
- Follow project code style guidelines (if any)
- Use `clang-format` for consistent formatting
- Compilation warnings are forbidden

### 6.2 Build Verification
```bash
# Local pre-commit check
mkdir build && cd build
cmake ..
make -j$(nproc)
make test  # Run tests
```

### 6.3 Dependency Management
- Explain dependency updates in PRs
- Major version changes require separate evaluation

## 7. Conflict Resolution

### 7.1 Basic Principles
- Merge upstream changes in a timely manner
- Make small commits to reduce conflicts
- Test thoroughly after resolving conflicts

### 7.2 Resolution Steps
```bash
# 1. Pull latest code
git fetch origin
git rebase origin/develop

# 2. Resolve conflicts
# Edit conflicting files...

# 3. Continue rebase
git add .
git rebase --continue

# 4. Force push (feature branch)
git push origin feature/xxx --force-with-lease
```

## 8. Emergency Handling

### 8.1 Production Issues
1. Immediately create a `hotfix` branch
2. Prioritize fixing the issue
3. Follow the hotfix workflow
4. Perform root cause analysis afterwards

### 8.2 CI/CD Failures
1. Check pipeline logs
2. Roll back the problematic commit
3. Fix the build script
4. Re-trigger deployment

## 9. Best Practices

### 9.1 Branch Management
- Delete merged branches promptly
- Keep branch lifespans short
- Avoid accumulating large changes on temporary branches

### 9.2 Commit Strategy
- Atomic commits (one complete change per commit)
- Meaningful commit messages
- Commit frequently, push regularly

### 9.3 Code Review
- Take review comments seriously
- Review others' code carefully
- Use GitLab's comment feature for discussions

---

## Changelog

| Version | Date | Description | Author |
|---------|------|-------------|--------|
| 1.0 | 2026-01-27 | Initial version | wzycc |

---

**Note**: This guideline serves as the foundation for team collaboration. Adjustments can be made flexibly under special circumstances, but must be discussed and agreed upon by the team. If you have any questions, consult the technical lead promptly.
