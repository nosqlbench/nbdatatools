# Cascade AI Assistant Conventions

## General Guidelines
- Always suggest your strategy and get confirmation before making changes
- Respond to questions before proceeding with implementation
- Prefer iterative changes with frequent check-ins
- Maintain a clean commit history with meaningful messages
- Keep changes atomic and focused to enable easy reversion if needed

## Code Style
- Follow existing patterns and paradigms in the codebase
- Prefer modifying existing files over creating new ones when appropriate
- Maintain consistent formatting with the existing codebase
- Document complex logic with clear comments
- Ensure all files pass linting with no errors or warnings before considering them complete

## Git Workflow
- Use `git add .` when adding files (not specific filenames)
- Write clear, descriptive commit messages
- Keep commits focused and atomic
- Reference relevant issues in commit messages when applicable
- When asked to commit changes, automatically push them to the remote repository
- At the end of a task, suggest a pull request that includes:
  - Title: Include the ticket/issue number from the branch name (e.g., if branch is 'feature/PRTL-123', use "[PRTL-123] Brief description of changes")
  - Description with:
    - Purpose of the changes
    - Key changes made
    - Testing performed
    - Any special considerations for reviewers

## Testing
- Consider an effort complete only when test coverage has been created and verified
- Follow existing test patterns in the codebase
- Ensure tests are reliable and not flaky
- Include appropriate test data and mocks

## Security
- Never hardcode sensitive information
- Follow security best practices for the relevant technologies
- Validate all inputs and handle errors gracefully

## Performance
- Be mindful of performance implications
- Optimize only when necessary and with benchmarks
- Consider both runtime and build-time performance

## DebugPanel Component

The DebugPanel is a development-only component that provides visibility into the application's environment and state. It's automatically included in development builds but excluded in production.

### Requesting a DebugPanel

To get a DebugPanel added to your project, simply ask Cascade to create one. The assistant will analyze the project structure and implement the component following the existing codebase conventions.

Example request:
```
Create a new DebugPanel component that follows our project conventions with these features:
- Floating panel that can be moved around the screen
- Collapsible sections for different types of debug information
- Real-time display of console logs and network requests within the panel
- No need to use browser dev tools for basic debugging
- Clean, organized UI that doesn't interfere with the main application
```

### Features

- Displays all environment variables with `VITE_` or `REACT_APP_` prefixes
- Shows runtime information (viewport, screen size, etc.)
- Only visible in development mode (`process.env.NODE_ENV !== 'production'`)
- Draggable interface for flexible positioning
- Collapsible sections for better organization

### When to Use

- During development to inspect environment variables
- Debugging environment-specific issues
- Verifying runtime conditions
- Testing different screen sizes and device capabilities

### Removing the DebugPanel

When the DebugPanel is no longer needed, you can remove it by:

1. Deleting the component directory:
   ```bash
   rm -rf src/components/DebugPanel
   ```

2. Removing any imports of the DebugPanel from your application

3. The DebugPanel is automatically excluded from production builds, but you should still remove it from the codebase when it's no longer needed to keep the codebase clean.
- Provide context for recommendations
- Admit uncertainty when appropriate