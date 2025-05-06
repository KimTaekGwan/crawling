import { test, expect } from '@playwright/test';

test('test', async ({ page }) => {
  await page.goto('http://zgai.me/');
  await page.locator('#siteMapOpenBtn span').first().click();
  const page1Promise = page.waitForEvent('popup');
  await page.getByRole('link', { name: 'AI로 제작하기' }).click();
  const page1 = await page1Promise;
  await page1.locator('iframe').nth(1).contentFrame().getByRole('textbox', { name: '이메일 주소를 입력해주세요' }).click();
  await page1.locator('iframe').nth(1).contentFrame().getByRole('textbox', { name: '이메일 주소를 입력해주세요' }).fill('zgaiadmin');
  await page1.locator('iframe').nth(1).contentFrame().getByRole('textbox', { name: '이메일 주소를 입력해주세요' }).press('Tab');
  await page1.locator('iframe').nth(1).contentFrame().getByRole('textbox', { name: '비밀번호 입력' }).fill('weven00#!!');
  page1.once('dialog', dialog => {
    console.log(`Dialog message: ${dialog.message()}`);
    dialog.dismiss().catch(() => {});
  });
  await page1.locator('iframe').nth(1).contentFrame().getByRole('button', { name: '로그인' }).click();
  await page1.getByRole('textbox', { name: 'modoo 주소 입력' }).click();
  await page1.locator('#modooURL').fill('mapopsc');

  await page1.locator('#modooGenerateBtn').click();

  // 토스트 메시지 확인
  await expect(page1.locator('#toastWrap').getByText('비공개 사이트는 접근할 수 없습니다. 공개 사이트만 이용 가능합니다.')).toBeVisible();
});