-- Issue #315: a `recompute` ring row may now carry a prior-image hint in
-- `old_image` (`staging::append::StagedChange::Recompute::prior_image`): the
-- row as it stood before the target write that staged the recompute, so a
-- downstream aggregate can re-derive the group that row left. It is still an
-- image-less trigger for the fold (`staging::fold` excludes `op = 'recompute'`
-- from both image arg-extremes and reads the hint into its own column), so
-- only `new_image` stays forbidden on it. A truncate sentinel stays fully
-- image-less.

alter table seg_0 drop constraint seg_0_recompute_has_no_images;
alter table seg_0 add constraint seg_0_recompute_has_no_images
    check (
        (op <> 'truncate' or (old_image is null and new_image is null))
        and (op <> 'recompute' or new_image is null)
    );

alter table seg_1 drop constraint seg_1_recompute_has_no_images;
alter table seg_1 add constraint seg_1_recompute_has_no_images
    check (
        (op <> 'truncate' or (old_image is null and new_image is null))
        and (op <> 'recompute' or new_image is null)
    );

alter table seg_2 drop constraint seg_2_recompute_has_no_images;
alter table seg_2 add constraint seg_2_recompute_has_no_images
    check (
        (op <> 'truncate' or (old_image is null and new_image is null))
        and (op <> 'recompute' or new_image is null)
    );

alter table seg_3 drop constraint seg_3_recompute_has_no_images;
alter table seg_3 add constraint seg_3_recompute_has_no_images
    check (
        (op <> 'truncate' or (old_image is null and new_image is null))
        and (op <> 'recompute' or new_image is null)
    );
