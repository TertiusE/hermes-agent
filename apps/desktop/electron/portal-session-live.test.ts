import { execFile } from 'node:child_process'
import { existsSync } from 'node:fs'
import { mkdtemp, rm } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { delimiter, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'

import { build } from 'esbuild'
import { expect, test } from 'vitest'

// Chromium needs an X11 display for this isolated fixture. On Linux, use
// xvfb-run whenever it is installed so the test is hermetic and never inherits
// a real X11/Wayland session without its matching authentication credentials.
// A real X11 display is only the fallback when it has an Xauthority path.
// Resolved once so the skip is explicit: `null` means no valid display path is
// available, otherwise this is the command prefix to use.
const displayPrefix = (() => {
  if (process.platform !== 'linux') {
    return []
  }

  const xvfbRun = (process.env.PATH ?? '')
    .split(delimiter)
    .map(dir => join(dir, 'xvfb-run'))
    .find(existsSync)

  if (xvfbRun) {
    return [xvfbRun, '-a']
  }

  return process.env.DISPLAY && process.env.XAUTHORITY ? [] : null
})()

test.skipIf(displayPrefix === null)(
  'real portal windows handle both providers, transitions and expired/rejected access',
  async () => {
    const root = await mkdtemp(join(tmpdir(), 'hermes-portal-session-'))

    try {
      const bundle = join(root, 'main.cjs')
      await build({
        entryPoints: [fileURLToPath(new URL('./portal-session-live-fixture/main.ts', import.meta.url))],
        outfile: bundle,
        bundle: true,
        platform: 'node',
        format: 'cjs',
        external: ['electron']
      })
      const env: NodeJS.ProcessEnv = {}

      for (const name of ['PATH', 'SystemRoot', 'WINDIR', 'DISPLAY', 'XAUTHORITY', 'WAYLAND_DISPLAY', 'XDG_RUNTIME_DIR']) {
        if (process.env[name]) {
          env[name] = process.env[name]
        }
      }

      const electron: string = createRequire(import.meta.url)('electron')
      // Chromium switches go after the fixture's positional root so
      // `process.argv[2]` in the fixture stays the root. `--no-sandbox` matches
      // the Playwright fixture: the npm-installed chrome-sandbox helper is not
      // setuid and Ubuntu 24.04 runners restrict unprivileged user namespaces,
      // so a sandboxed launch aborts before the main script runs.
      const [command, ...args] = [...(displayPrefix ?? []), electron, bundle, root, '--no-sandbox', '--disable-gpu']

      let stdout = ''

      try {
        stdout = (
          await promisify(execFile)(command, args, {
            env: { ...env, HERMES_HOME: join(root, '.hermes'), XDG_CONFIG_HOME: join(root, 'config') },
            timeout: 45_000
          })
        ).stdout
      } catch (error) {
        // execFile's rejection carries only "Command failed"; Electron's real
        // reason (sandbox abort, missing libs, fixture assertion) is on stderr.
        const { stderr = '', stdout: partial = '' } = error as { stderr?: string; stdout?: string }
        throw new Error(`Electron fixture failed.\n--- stdout ---\n${partial}\n--- stderr ---\n${stderr}`, {
          cause: error
        })
      }

      expect(stdout).toContain('PORTAL_SESSION_LIVE_OK')
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  },
  60_000
)
