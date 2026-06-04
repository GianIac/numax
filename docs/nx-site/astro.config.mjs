import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://numax.run',
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
      sidebar: [
        {
          label: 'Getting Started',
          translations: { it: 'Per iniziare' },
          items: [
            {
              label: 'Introduction',
              translations: { it: 'Introduzione' },
              slug: 'getting-started/introduction',
            },
            {
              label: 'Installation',
              translations: { it: 'Installazione' },
              slug: 'getting-started/installation',
            },
            {
              label: 'Quickstart: 5 minutes',
              translations: { it: 'Quickstart: 5 minuti' },
              slug: 'getting-started/quickstart-5-min',
            },
            {
              label: 'Your first module',
              translations: { it: 'Il tuo primo modulo' },
              slug: 'getting-started/your-first-module',
            },
          ],
        },
        {
          label: 'Concepts',
          translations: { it: 'Concetti' },
          items: [
            {
              label: 'Runtime model',
              translations: { it: 'Modello runtime' },
              slug: 'concepts/runtime-model',
            },
            {
              label: 'WASM execution',
              translations: { it: 'Esecuzione WASM' },
              slug: 'concepts/wasm-execution',
            },
            {
              label: 'CRDT and state',
              translations: { it: 'CRDT e stato' },
              slug: 'concepts/crdt-and-state',
            },
            {
              label: 'Gossip protocol',
              translations: { it: 'Protocollo gossip' },
              slug: 'concepts/gossip-protocol',
            },
            { label: 'Local-first', translations: { it: 'Local-first' }, slug: 'concepts/local-first' },
          ],
        },
        {
          label: 'Guides',
          translations: { it: 'Guide' },
          items: [
            {
              label: 'Build a KV store',
              translations: { it: 'Costruire un KV store' },
              slug: 'guides/build-a-kv-store',
            },
            {
              label: 'Deploy on edge',
              translations: { it: 'Deploy su edge' },
              slug: 'guides/deploy-on-edge',
            },
            {
              label: 'Writing host functions',
              translations: { it: 'Scrivere host function' },
              slug: 'guides/writing-host-functions',
            },
            {
              label: 'Debugging WASM modules',
              translations: { it: 'Debug dei moduli WASM' },
              slug: 'guides/debugging-wasm-modules',
            },
          ],
        },
        {
          label: 'Reference',
          translations: { it: 'Reference' },
          items: [
            { label: 'Host API', slug: 'reference/host-api' },
            { label: 'CLI', slug: 'reference/cli' },
            {
              label: 'Configuration',
              translations: { it: 'Configurazione' },
              slug: 'reference/config',
            },
            {
              label: 'Crates',
              translations: { it: 'Crate' },
              items: [
                {
                  label: 'Overview',
                  translations: { it: 'Panoramica' },
                  slug: 'reference/crates',
                },
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
          translations: { it: 'Cookbook' },
          items: [{ label: 'Recipes', translations: { it: 'Ricette' }, slug: 'cookbook' }],
        },
        {
          label: 'Project',
          translations: { it: 'Progetto' },
          items: [
            { label: 'Blog', slug: 'blog' },
            { label: 'Roadmap', slug: 'roadmap' },
            { label: 'Whitepaper', slug: 'whitepaper' },
            { label: 'Playground', slug: 'playground' },
            { label: 'Community', translations: { it: 'Community' }, slug: 'community' },
            { label: 'Showcase', slug: 'showcase' },
          ],
        },
      ],
    }),
  ],
});
