I needed an efficient way to store deltas between ntfsclone images.

It turned out to be particularly easy because the file format of the 
ntfsclone images is pretty straight forward. They are in essence just 
a series of bytes saying either '1: here comes the next ntfs cluster', 
followed by one ntfs cluster, or '0: the next cluster was unused', 
followed by nothing. My delta file uses exactly the same file format, 
adding a third case '2: the next cluster was identical in both images', 
followed by nothing.

  Usage:
  ntfscloneimgdelta delta OLDFILE [NEWFILE [DELTA]]
  ntfscloneimgdelta patch OLDFILE [DELTA [NEWFILE]]

OLDFILE and NEWFILE do not need to be in ascending chronological order,
you can swap them to create reverse deltas. This enables you to always 
keep the latest backup as a full dump, while keeping older dumps as deltas.

Omitting file names or replacing them with '-' uses stdin or stdout. This
allows to take a new dump of a partition and create a delta between it and
another dump in one go.
