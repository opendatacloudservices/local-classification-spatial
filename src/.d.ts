declare module 'string-comparison' {
  type comparison = (str_1: string, str_2: string) => number;
  type sortMatch = (str_1: string, strings: string[]) => {
    member: string;
    index: number;
    rating: number;
  }[];

  export const levenshtein: {
    similarity: comparison,
    distance: comparison,
    sortMatch: sortMatch 
  };
  
  export const jaccardIndex: {
    similarity: comparison,
    distance: comparison,
    sortMatch: sortMatch
  };
}
