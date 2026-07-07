import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://gianiac.github.io',
  base: '/numax',
  // Italian docs are not translated yet. Each IT page is a "Coming soon"
  // stub under src/content/docs/it/. The language selector remains visible
  // so users can switch back to English from any IT page.
  integrations: [
    starlight({
      title: 'Numax',
      description: 'A portable runtime for local-first distributed WASM apps.',
      defaultLocale: 'root',
      locales: {
        root: {
          label: 'English',
          lang: 'en',
        },
        it: {
          label: 'Italiano',
          lang: 'it',
        },
      },
      editLink: {
        baseUrl: 'https://github.com/GianIac/numax/edit/main/docs/nx-site/',
      },
      social: {
        github: 'https://github.com/GianIac/numax',
      },
      components: {
        Header: './src/components/Header.astro',
      },
      sidebar: [
        {
          label: 'Getting Started',
          items: [
            { label: 'Introduction', slug: 'getting-started/introduction' },
            { label: 'Installation', slug: 'getting-started/installation' },
            { label: 'Quickstart: 5 minutes', slug: 'getting-started/quickstart-5-min' },
            { label: 'Your first module', slug: 'getting-started/your-first-module' },
          ],
        },
        {
          label: 'Concepts',
          items: [
            { label: 'Foundations', slug: 'concepts/foundations' },
            { label: 'Runtime model', slug: 'concepts/runtime-model' },
            { label: 'WASM execution', slug: 'concepts/wasm-execution' },
            { label: 'CRDT and state', slug: 'concepts/crdt-and-state' },
            { label: 'Gossip protocol', slug: 'concepts/gossip-protocol' },
            { label: 'Local-first', slug: 'concepts/local-first' },
          ],
        },
        {
          label: 'Guides',
          items: [
            { label: 'Build a KV store', slug: 'guides/build-a-kv-store' },
            { label: 'Writing host functions', slug: 'guides/writing-host-functions' },
            { label: 'Debugging WASM modules', slug: 'guides/debugging-wasm-modules' },
            { label: 'Observability', slug: 'guides/observability' },
          ],
        },
        {
          label: 'Design',
          items: [
            { label: 'Schema versioning', slug: 'design/schema-versioning' },
            { label: 'Wire versioning', slug: 'design/wire-versioning' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'Host API', slug: 'reference/host-api' },
            { label: 'CLI', slug: 'reference/cli' },
            { label: 'Configuration', slug: 'reference/config' },
            {
              label: 'Crates',
              items: [
                { label: 'Overview', slug: 'reference/crates' },
                { label: 'nx-cli', slug: 'reference/crates/nx-cli' },
                { label: 'nx-core', slug: 'reference/crates/nx-core' },
                { label: 'nx-net', slug: 'reference/crates/nx-net' },
                { label: 'nx-sdk', slug: 'reference/crates/nx-sdk' },
                { label: 'nx-store', slug: 'reference/crates/nx-store' },
                { label: 'nx-sync', slug: 'reference/crates/nx-sync' },
              ],
            },
          ],
        },
        {
          label: 'Cookbook',
          items: [{ label: 'Recipes', slug: 'cookbook' }],
        },
        {
          label: 'Project',
          items: [
            { label: 'Blog', slug: 'blog' },
            { label: 'Roadmap', slug: 'roadmap' },
            { label: 'Whitepaper', slug: 'whitepaper' },
            { label: 'Community', slug: 'community' },
            { label: 'Showcase', slug: 'showcase' },
          ],
        },
      ],
    }),
  ],
});
