import { ComponentFixture, TestBed } from '@angular/core/testing';
import { GraphEditorComponent } from './graph-editor';
import { emptyVm, toNodeJson, toNodeVm } from './graph-model';

/**
 * WP12 baseline (characterisation) tests for the graph editor: they pin the node mapping and the
 * key structural interactions the refactoring must preserve.
 */
describe('GraphEditorComponent (baseline)', () => {
  let fixture: ComponentFixture<GraphEditorComponent>;
  let component: GraphEditorComponent;

  beforeEach(async () => {
    await TestBed.configureTestingModule({ imports: [GraphEditorComponent] }).compileComponents();
    fixture = TestBed.createComponent(GraphEditorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('renders nodes passed via nodesJson', () => {
    fixture.componentRef.setInput('nodesJson', [
      { id: 'a', type: 'agent', title: 'Alpha', agent: { ref: 'agent-1' } },
      { id: 'b', type: 'agent', title: 'Beta', dependsOn: ['a'] },
    ]);
    fixture.detectChanges();

    expect(component.nodes().length).toBe(2);
    const text: string = fixture.nativeElement.textContent;
    expect(text).toContain('Alpha');
    expect(text).toContain('Beta');
  });

  it('adds a node and emits the rebuilt node array', () => {
    const emitted: unknown[][] = [];
    component.nodesChange.subscribe((v) => emitted.push(v));

    component.newType = 'loop';
    component.addNode();
    fixture.detectChanges();

    expect(component.nodes().length).toBe(1);
    expect(component.nodes()[0].type).toBe('loop');
    expect(emitted.length).toBe(1);
  });

  it('connects two nodes via click-to-link', () => {
    const emitted: unknown[][] = [];
    component.nodesChange.subscribe((v) => emitted.push(v));
    fixture.componentRef.setInput('nodesJson', [
      { id: 'a', type: 'agent' },
      { id: 'b', type: 'agent' },
    ]);
    fixture.detectChanges();

    component.linking.set(true);
    component.onNodeClick('a');
    component.onNodeClick('b');
    fixture.detectChanges();

    expect(emitted.length).toBe(1);
    const out = emitted[0] as Array<{ id: string; dependsOn?: string[] }>;
    expect(out.find((n) => n.id === 'b')?.dependsOn).toEqual(['a']);
  });

  it('deletes a node and prunes dangling references', () => {
    fixture.componentRef.setInput('nodesJson', [
      { id: 'a', type: 'agent' },
      { id: 'b', type: 'agent', dependsOn: ['a'] },
    ]);
    fixture.detectChanges();

    component.onNodeClick('a');
    component.deleteSelected();
    fixture.detectChanges();

    expect(component.nodes().map((n) => n.id)).toEqual(['b']);
    expect(component.nodes()[0].dependsOn).toEqual([]);
  });

  it('editing the ID field changes the id but leaves references untouched (current behaviour)', () => {
    // Characterisation: `rename()` early-returns because `selected()` looks up by the *old*
    // selectedId after ngModel has already written the new id, so the reference rewrite never
    // runs. WP12 freezes this behaviour; a follow-up can fix the dangling reference.
    fixture.componentRef.setInput('nodesJson', [
      { id: 'a', type: 'agent' },
      { id: 'b', type: 'agent', dependsOn: ['a'] },
    ]);
    fixture.detectChanges();

    const node = fixture.nativeElement.querySelectorAll('.gnode')[0] as HTMLElement;
    node.dispatchEvent(new MouseEvent('click'));
    fixture.detectChanges();

    const idInput = fixture.nativeElement.querySelector('.card input') as HTMLInputElement;
    idInput.value = 'a2';
    idInput.dispatchEvent(new Event('input'));
    idInput.dispatchEvent(new Event('change'));
    fixture.detectChanges();

    expect(component.nodes().map((n) => n.id)).toEqual(['a2', 'b']);
    expect(component.nodes().find((n) => n.id === 'b')!.dependsOn).toEqual(['a']);
  });
});

describe('graph-model', () => {
  it('toNodeVm maps agent refs and branches', () => {
    const vm = toNodeVm({
      id: 'd',
      type: 'decision',
      title: 'Decide',
      agent: { ref: 'a1' },
      dependsOn: ['start'],
      branches: [{ branchId: 'b1', condition: 'x>1', firstStep: 'next' }],
    });
    expect(vm.agentRef).toBe('a1');
    expect(vm.dependsOn).toEqual(['start']);
    expect(vm.branches).toEqual([{ branchId: 'b1', condition: 'x>1', target: 'next' }]);
  });

  it('toNodeJson emits the schema-shaped node per type', () => {
    const agent = emptyVm('a', 'agent');
    expect(toNodeJson(agent)).toEqual({ id: 'a', type: 'agent', title: 'a', agent: { ref: '' } });

    const decision = emptyVm('d', 'decision');
    expect(toNodeJson(decision)).toEqual({
      id: 'd',
      type: 'decision',
      title: 'd',
      branches: [{ branchId: 'b1', condition: '', target: '' }],
    });

    const loop = toNodeJson({ ...emptyVm('l', 'loop'), exitCondition: '' });
    expect(loop).toEqual({ id: 'l', type: 'loop', title: 'l', maxIterations: 3, exitCondition: 'falsy', bodySteps: [] });
  });
});
