import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test('workflow smoke: navigate routes and create intent placeholders', async ({ page }) => {
  await page.goto('/dashboard');
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible();

  await page.getByRole('link', { name: 'Cohort Builder' }).click();
  await expect(page.getByRole('heading', { name: 'Cohort Builder' })).toBeVisible();
  await page.getByRole('button', { name: 'Create Generation Intent' }).click();
  await expect(page.getByText('Queued Job')).toBeVisible();

  await page.getByRole('link', { name: 'Model Catalog' }).click();
  await expect(page.getByRole('heading', { name: 'Model Catalog' })).toBeVisible();
  await expect(page.getByText('CoxCOPDInputItem')).toBeVisible();

  await page.getByRole('link', { name: 'Run History' }).click();
  await expect(page.getByRole('heading', { name: 'Run History' })).toBeVisible();
  await expect(page.getByText('Run Result')).toBeVisible();
});

test('dashboard has no critical accessibility violations', async ({ page }) => {
  await page.goto('/dashboard');
  const accessibilityScanResults = await new AxeBuilder({ page }).analyze();
  expect(accessibilityScanResults.violations).toEqual([]);
});
