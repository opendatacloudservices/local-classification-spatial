export const date2timestamp = (date: Date): string => {
  return `${appendZero(date.getFullYear())}-${appendZero(
    date.getMonth()
  )}-${appendZero(date.getDay())} ${appendZero(date.getHours())}:${appendZero(
    date.getMinutes()
  )}:${appendZero(date.getSeconds())}`;
};

export const appendZero = (num: number): string => {
  return num < 10 ? '0' + num.toString() : num.toString();
};

export const countDecimals = (num: number): number => {
  if (Math.floor(num.valueOf()) === num.valueOf()) return 0;
  return num.toString().split('.')[1].length || 0;
};
