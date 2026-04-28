import React from "react";

type ProfileDrawerProps = {
  geographyId: string;
  score: number;
  recommendation: string;
};

export function ProfileDrawer({
  geographyId,
  score,
  recommendation,
}: ProfileDrawerProps): JSX.Element {
  return (
    <div className="profile-drawer">
      <h3>Profile</h3>
      <div className="profile-row">
        <span>Geography</span>
        <strong>{geographyId}</strong>
      </div>
      <div className="profile-row">
        <span>Opportunity</span>
        <strong>{score.toFixed(1)}</strong>
      </div>
      <div className="profile-row">
        <span>Recommendation</span>
        <strong>{recommendation}</strong>
      </div>
    </div>
  );
}
