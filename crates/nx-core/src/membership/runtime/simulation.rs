use super::*;

pub(crate) struct Simulation(Actor);
impl Simulation {
    pub(crate) fn new(
        local: &str,
        endpoint: &str,
        node: Arc<dyn crate::peer_io::PeerIo>,
        store: Arc<Store>,
        metrics: Arc<RuntimeMetrics>,
        seed: u64,
    ) -> anyhow::Result<Self> {
        let (core, owner) = initialize(local.into(), Some(endpoint.into()), &store)?;
        let (_, incoming) = mpsc::channel(64);
        let mut actor = Actor::new(core, owner, node, store, incoming, metrics, seed);
        actor.jobs.manual();
        actor.writes.manual();
        actor.publish_snapshot();
        Ok(Self(actor))
    }
    pub(crate) fn snapshot(&self) -> watch::Receiver<Snapshot> {
        self.0.snapshot_tx.subscribe()
    }
    pub(crate) async fn observe(&mut self, event: Observation) -> anyhow::Result<()> {
        match &event {
            Observation::Written(_) => self.0.writes.finished(),
            Observation::Completed(_) => self.0.jobs.finished(),
            _ => {}
        }
        self.0.observe(event).await
    }
    pub(crate) fn dispatches(&mut self) -> Vec<Observation> {
        let mut result = Vec::new();
        while let Ok(dispatch) = self.0.dispatch_rx.try_recv() {
            result.push(Observation::Dispatch(dispatch));
        }
        result
    }
    pub(crate) fn jobs(&mut self) -> Vec<Pin<Box<dyn Future<Output = Observation> + Send>>> {
        self.0
            .jobs
            .take()
            .into_iter()
            .map(|job| {
                Box::pin(async move { Observation::Completed(Box::new(job.await)) })
                    as Pin<Box<dyn Future<Output = Observation> + Send>>
            })
            .collect()
    }
    pub(crate) fn writes(&mut self) -> Vec<Pin<Box<dyn Future<Output = Observation> + Send>>> {
        self.0
            .writes
            .take()
            .into_iter()
            .map(|work| {
                Box::pin(async move { Observation::Written(Written(work.await)) })
                    as Pin<Box<dyn Future<Output = Observation> + Send>>
            })
            .collect()
    }
    pub(crate) async fn leave(&mut self) -> anyhow::Result<()> {
        let actions = self.0.core.leave().map_err(core_error)?;
        self.0.actions(actions).await?;
        self.0.publish_snapshot();
        Ok(())
    }
}
